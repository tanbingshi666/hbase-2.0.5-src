/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.hbase.zookeeper.MasterAddressTracker;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ZNodeClearer;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.zookeeper.ZKListener;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles everything on master-side related to master election.
 *
 * <p>Listens and responds to ZooKeeper notifications on the master znode,
 * both <code>nodeCreated</code> and <code>nodeDeleted</code>.
 *
 * <p>Contains blocking methods which will hold up backup masters, waiting
 * for the active master to fail.
 *
 * <p>This class is instantiated in the HMaster constructor and the method
 * #blockUntilBecomingActiveMaster() is called to wait until becoming
 * the active master of the cluster.
 */
@InterfaceAudience.Private
public class ActiveMasterManager extends ZKListener {
    private static final Logger LOG = LoggerFactory.getLogger(ActiveMasterManager.class);

    final AtomicBoolean clusterHasActiveMaster = new AtomicBoolean(false);
    final AtomicBoolean clusterShutDown = new AtomicBoolean(false);

    private final ServerName sn;
    private int infoPort;
    private final Server master;

    /**
     * @param watcher
     * @param sn      ServerName
     * @param master  In an instance of a Master.
     */
    ActiveMasterManager(ZKWatcher watcher, ServerName sn, Server master) {
        // watcher = ZKWatcher 调用父类 ZKListener
        super(watcher);
        // 注册监听器
        watcher.registerListener(this);
        this.sn = sn;
        // HMaster
        this.master = master;
    }

    // will be set after jetty server is started
    public void setInfoPort(int infoPort) {
        this.infoPort = infoPort;
    }

    @Override
    public void nodeCreated(String path) {
        handle(path);
    }

    @Override
    public void nodeDeleted(String path) {

        // We need to keep track of the cluster's shutdown status while
        // we wait on the current master. We consider that, if the cluster
        // was already in a "shutdown" state when we started, that this master
        // is part of a new cluster that was started shortly after the old cluster
        // shut down, so that state is now irrelevant. This means that the shutdown
        // state must be set while we wait on the active master in order
        // to shutdown this master. See HBASE-8519.
        if (path.equals(watcher.znodePaths.clusterStateZNode) && !master.isStopped()) {
            clusterShutDown.set(true);
        }

        handle(path);
    }

    void handle(final String path) {
        if (path.equals(watcher.znodePaths.masterAddressZNode) && !master.isStopped()) {
            handleMasterNodeChange();
        }
    }

    /**
     * Handle a change in the master node.  Doesn't matter whether this was called
     * from a nodeCreated or nodeDeleted event because there are no guarantees
     * that the current state of the master node matches the event at the time of
     * our next ZK request.
     *
     * <p>Uses the watchAndCheckExists method which watches the master address node
     * regardless of whether it exists or not.  If it does exist (there is an
     * active master), it returns true.  Otherwise it returns false.
     *
     * <p>A watcher is set which guarantees that this method will get called again if
     * there is another change in the master node.
     */
    private void handleMasterNodeChange() {
        // Watch the node and check if it exists.
        try {
            synchronized (clusterHasActiveMaster) {
                if (ZKUtil.watchAndCheckExists(watcher, watcher.znodePaths.masterAddressZNode)) {
                    // A master node exists, there is an active master
                    LOG.trace("A master is now available");
                    clusterHasActiveMaster.set(true);
                } else {
                    // Node is no longer there, cluster does not have an active master
                    LOG.debug("No master available. Notifying waiting threads");
                    clusterHasActiveMaster.set(false);
                    // Notify any thread waiting to become the active master
                    clusterHasActiveMaster.notifyAll();
                }
            }
        } catch (KeeperException ke) {
            master.abort("Received an unexpected KeeperException, aborting", ke);
        }
    }

    /**
     * Block until becoming the active master.
     * <p>
     * Method blocks until there is not another active master and our attempt
     * to become the new active master is successful.
     * <p>
     * This also makes sure that we are watching the master znode so will be
     * notified if another master dies.
     *
     * @param checkInterval the interval to check if the master is stopped
     * @param startupStatus the monitor status to track the progress
     * @return True if no issue becoming active master else false if another
     * master was running or if some other problem (zookeeper, stop flag has been
     * set on this Master)
     */
    boolean blockUntilBecomingActiveMaster(
            int checkInterval, MonitoredTask startupStatus) {
        String backupZNode = ZNodePaths.joinZNode(
                this.watcher.znodePaths.backupMasterAddressesZNode, this.sn.toString());
        while (!(master.isAborted() || master.isStopped())) {
            startupStatus.setStatus("Trying to register in ZK as active master");
            // Try to become the active master, watch if there is another master.
            // Write out our ServerName as versioned bytes.
            try {
                // 创建在 ZKActive HMaster 临时节点
                if (MasterAddressTracker.setMasterAddress(this.watcher,
                        this.watcher.znodePaths.masterAddressZNode, this.sn, infoPort)) {

                    // If we were a backup master before, delete our ZNode from the backup
                    // master directory since we are the active now)
                    // 删除 backup HMaster 临时节点
                    if (ZKUtil.checkExists(this.watcher, backupZNode) != -1) {
                        /**
                         * Deleting ZNode for
                         * /hbase/backup-masters/hadoop102,16000,1685842623795 from backup master directory
                         */
                        LOG.info("Deleting ZNode for " + backupZNode + " from backup master directory");
                        ZKUtil.deleteNodeFailSilent(this.watcher, backupZNode);
                    }
                    // Save the znode in a file, this will allow to check if we crash in the launch scripts
                    ZNodeClearer.writeMyEphemeralNodeOnDisk(this.sn.toString());

                    // We are the master, return
                    startupStatus.setStatus("Successfully registered as active master.");
                    this.clusterHasActiveMaster.set(true);
                    // Registered as active master=hadoop102,16000,1685842623795
                    LOG.info("Registered as active master=" + this.sn);
                    return true;
                }

                // There is another active master running elsewhere or this is a restart
                // and the master ephemeral node has not expired yet.
                this.clusterHasActiveMaster.set(true);

                String msg;
                byte[] bytes =
                        ZKUtil.getDataAndWatch(this.watcher, this.watcher.znodePaths.masterAddressZNode);
                if (bytes == null) {
                    msg = ("A master was detected, but went down before its address " +
                            "could be read.  Attempting to become the next active master");
                } else {
                    ServerName currentMaster;
                    try {
                        currentMaster = ProtobufUtil.parseServerNameFrom(bytes);
                    } catch (DeserializationException e) {
                        LOG.warn("Failed parse", e);
                        // Hopefully next time around we won't fail the parse.  Dangerous.
                        continue;
                    }
                    if (ServerName.isSameAddress(currentMaster, this.sn)) {
                        msg = ("Current master has this master's address, " +
                                currentMaster + "; master was restarted? Deleting node.");
                        // Hurry along the expiration of the znode.
                        ZKUtil.deleteNode(this.watcher, this.watcher.znodePaths.masterAddressZNode);

                        // We may have failed to delete the znode at the previous step, but
                        //  we delete the file anyway: a second attempt to delete the znode is likely to fail again.
                        ZNodeClearer.deleteMyEphemeralNodeOnDisk();
                    } else {
                        msg = "Another master is the active master, " + currentMaster +
                                "; waiting to become the next active master";
                    }
                }
                LOG.info(msg);
                startupStatus.setStatus(msg);
            } catch (KeeperException ke) {
                master.abort("Received an unexpected KeeperException, aborting", ke);
                return false;
            }
            synchronized (this.clusterHasActiveMaster) {
                while (clusterHasActiveMaster.get() && !master.isStopped()) {
                    try {
                        clusterHasActiveMaster.wait(checkInterval);
                    } catch (InterruptedException e) {
                        // We expect to be interrupted when a master dies,
                        //  will fall out if so
                        LOG.debug("Interrupted waiting for master to die", e);
                    }
                }
                if (clusterShutDown.get()) {
                    this.master.stop(
                            "Cluster went down before this master became active");
                }
            }
        }
        return false;
    }

    /**
     * @return True if cluster has an active master.
     */
    boolean hasActiveMaster() {
        try {
            if (ZKUtil.checkExists(watcher, watcher.znodePaths.masterAddressZNode) >= 0) {
                return true;
            }
        } catch (KeeperException ke) {
            LOG.info("Received an unexpected KeeperException when checking " +
                    "isActiveMaster : " + ke);
        }
        return false;
    }

    public void stop() {
        try {
            synchronized (clusterHasActiveMaster) {
                // Master is already stopped, wake up the manager
                // thread so that it can shutdown soon.
                clusterHasActiveMaster.notifyAll();
            }
            // If our address is in ZK, delete it on our way out
            ServerName activeMaster = null;
            try {
                activeMaster = MasterAddressTracker.getMasterAddress(this.watcher);
            } catch (IOException e) {
                LOG.warn("Failed get of master address: " + e.toString());
            }
            if (activeMaster != null && activeMaster.equals(this.sn)) {
                ZKUtil.deleteNode(watcher, watcher.znodePaths.masterAddressZNode);
                // We may have failed to delete the znode at the previous step, but
                //  we delete the file anyway: a second attempt to delete the znode is likely to fail again.
                ZNodeClearer.deleteMyEphemeralNodeOnDisk();
            }
        } catch (KeeperException e) {
            LOG.debug(this.watcher.prefix("Failed delete of our master address node; " +
                    e.getMessage()));
        }
    }
}
