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
package org.apache.hadoop.hbase.ipc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.apache.hadoop.hbase.conf.ConfigurationObserver;

/**
 * The default scheduler. Configurable. Maintains isolated handler pools for general ('default'),
 * high-priority ('priority'), and replication ('replication') requests. Default behavior is to
 * balance the requests across handlers. Add configs to enable balancing by read vs writes, etc.
 * See below article for explanation of options.
 *
 * @see <a href="http://blog.cloudera.com/blog/2014/12/new-in-cdh-5-2-improvements-for-running-multiple-workloads-on-a-single-hbase-cluster/">Overview on Request Queuing</a>
 */
@InterfaceAudience.LimitedPrivate({HBaseInterfaceAudience.COPROC, HBaseInterfaceAudience.PHOENIX})
@InterfaceStability.Evolving
public class SimpleRpcScheduler extends RpcScheduler implements ConfigurationObserver {
    private int port;
    private final PriorityFunction priority;
    private final RpcExecutor callExecutor;
    private final RpcExecutor priorityExecutor;
    private final RpcExecutor replicationExecutor;

    /**
     * This executor is only for meta transition
     */
    private final RpcExecutor metaTransitionExecutor;

    /**
     * What level a high priority call is at.
     */
    private final int highPriorityLevel;

    private Abortable abortable = null;

    /**
     * @param conf
     * @param handlerCount            the number of handler threads that will be used to process calls
     * @param priorityHandlerCount    How many threads for priority handling.
     * @param replicationHandlerCount How many threads for replication handling.
     * @param highPriorityLevel
     * @param priority                Function to extract request priority.
     */
    public SimpleRpcScheduler(
            Configuration conf,
            int handlerCount,
            int priorityHandlerCount,
            int replicationHandlerCount,
            int metaTransitionHandler,
            PriorityFunction priority,
            Abortable server,
            int highPriorityLevel) {

        // 默认 30 * 10
        int maxQueueLength = conf.getInt(RpcScheduler.IPC_SERVER_MAX_CALLQUEUE_LENGTH,
                handlerCount * RpcServer.DEFAULT_MAX_CALLQUEUE_LENGTH_PER_HANDLER);
        int maxPriorityQueueLength =
                conf.getInt(RpcScheduler.IPC_SERVER_PRIORITY_MAX_CALLQUEUE_LENGTH, maxQueueLength);

        this.priority = priority;
        this.highPriorityLevel = highPriorityLevel;
        this.abortable = server;

        String callQueueType = conf.get(RpcExecutor.CALL_QUEUE_TYPE_CONF_KEY,
                RpcExecutor.CALL_QUEUE_TYPE_CONF_DEFAULT);
        float callqReadShare = conf.getFloat(RWQueueRpcExecutor.CALL_QUEUE_READ_SHARE_CONF_KEY, 0);

        if (callqReadShare > 0) {
            // at least 1 read handler and 1 write handler
            callExecutor = new RWQueueRpcExecutor("default.RWQ", Math.max(2, handlerCount),
                    maxQueueLength, priority, conf, server);
        } else {
            if (RpcExecutor.isFifoQueueType(callQueueType) || RpcExecutor.isCodelQueueType(callQueueType)) {
                // 创建 FastPathBalancedQueueRpcExecutor
                callExecutor = new FastPathBalancedQueueRpcExecutor(
                        "default.FPBQ",
                        handlerCount,
                        maxQueueLength,
                        priority,
                        conf,
                        server);
            } else {
                callExecutor = new BalancedQueueRpcExecutor("default.BQ", handlerCount, maxQueueLength,
                        priority, conf, server);
            }
        }

        // Create 2 queues to help priorityExecutor be more scalable.
        this.priorityExecutor = priorityHandlerCount > 0 ? new FastPathBalancedQueueRpcExecutor(
                "priority.FPBQ", priorityHandlerCount, RpcExecutor.CALL_QUEUE_TYPE_FIFO_CONF_VALUE,
                maxPriorityQueueLength, priority, conf, abortable) : null;
        this.replicationExecutor = replicationHandlerCount > 0 ? new FastPathBalancedQueueRpcExecutor(
                "replication.FPBQ", replicationHandlerCount, RpcExecutor.CALL_QUEUE_TYPE_FIFO_CONF_VALUE,
                maxQueueLength, priority, conf, abortable) : null;

        this.metaTransitionExecutor = metaTransitionHandler > 0 ?
                new FastPathBalancedQueueRpcExecutor("metaPriority.FPBQ", metaTransitionHandler,
                        RpcExecutor.CALL_QUEUE_TYPE_FIFO_CONF_VALUE, maxPriorityQueueLength, priority, conf,
                        abortable) :
                null;
    }

    public SimpleRpcScheduler(Configuration conf, int handlerCount, int priorityHandlerCount,
                              int replicationHandlerCount, PriorityFunction priority, int highPriorityLevel) {
        this(conf, handlerCount, priorityHandlerCount, replicationHandlerCount, 0, priority, null,
                highPriorityLevel);
    }

    /**
     * Resize call queues;
     *
     * @param conf new configuration
     */
    @Override
    public void onConfigurationChange(Configuration conf) {
        callExecutor.resizeQueues(conf);
        if (priorityExecutor != null) {
            priorityExecutor.resizeQueues(conf);
        }
        if (replicationExecutor != null) {
            replicationExecutor.resizeQueues(conf);
        }
        if (metaTransitionExecutor != null) {
            metaTransitionExecutor.resizeQueues(conf);
        }

        String callQueueType = conf.get(RpcExecutor.CALL_QUEUE_TYPE_CONF_KEY,
                RpcExecutor.CALL_QUEUE_TYPE_CONF_DEFAULT);
        if (RpcExecutor.isCodelQueueType(callQueueType)) {
            callExecutor.onConfigurationChange(conf);
        }
    }

    @Override
    public void init(Context context) {
        this.port = context.getListenerAddress().getPort();
    }

    @Override
    public void start() {
        // 启动 RpcExecutor 默认调用 FastPathBalancedQueueRpcExecutor.start()
        callExecutor.start(port);
        if (priorityExecutor != null) {
            priorityExecutor.start(port);
        }
        if (replicationExecutor != null) {
            replicationExecutor.start(port);
        }
        if (metaTransitionExecutor != null) {
            metaTransitionExecutor.start(port);
        }

    }

    @Override
    public void stop() {
        callExecutor.stop();
        if (priorityExecutor != null) {
            priorityExecutor.stop();
        }
        if (replicationExecutor != null) {
            replicationExecutor.stop();
        }
        if (metaTransitionExecutor != null) {
            metaTransitionExecutor.stop();
        }

    }

    @Override
    public boolean dispatch(CallRunner callTask) throws InterruptedException {
        // 获取 RPC 执行 一般为 NettyServerCall
        RpcCall call = callTask.getRpcCall();
        int level = priority.getPriority(call.getHeader(), call.getParam(),
                call.getRequestUser().orElse(null));
        if (level == HConstants.PRIORITY_UNSET) {
            level = HConstants.NORMAL_QOS;
        }
        if (metaTransitionExecutor != null && level == HConstants.META_QOS) {
            return metaTransitionExecutor.dispatch(callTask);
        } else if (priorityExecutor != null && level > highPriorityLevel) {
            return priorityExecutor.dispatch(callTask);
        } else if (replicationExecutor != null && level == HConstants.REPLICATION_QOS) {
            return replicationExecutor.dispatch(callTask);
        } else {
            // 默认调用 FastPathBalancedQueueRpcExecutor.dispatch()
            return callExecutor.dispatch(callTask);
        }
    }

    @Override
    public int getMetaPriorityQueueLength() {
        return metaTransitionExecutor == null ? 0 : metaTransitionExecutor.getQueueLength();
    }

    @Override
    public int getGeneralQueueLength() {
        return callExecutor.getQueueLength();
    }

    @Override
    public int getPriorityQueueLength() {
        return priorityExecutor == null ? 0 : priorityExecutor.getQueueLength();
    }

    @Override
    public int getReplicationQueueLength() {
        return replicationExecutor == null ? 0 : replicationExecutor.getQueueLength();
    }

    @Override
    public int getActiveRpcHandlerCount() {
        return callExecutor.getActiveHandlerCount() +
                (priorityExecutor == null ? 0 : priorityExecutor.getActiveHandlerCount()) +
                (replicationExecutor == null ? 0 : replicationExecutor.getActiveHandlerCount()) +
                (metaTransitionExecutor == null ? 0 : metaTransitionExecutor.getActiveHandlerCount());
    }

    @Override
    public long getNumGeneralCallsDropped() {
        return callExecutor.getNumGeneralCallsDropped();
    }

    @Override
    public long getNumLifoModeSwitches() {
        return callExecutor.getNumLifoModeSwitches();
    }

    @Override
    public int getWriteQueueLength() {
        return callExecutor.getWriteQueueLength();
    }

    @Override
    public int getReadQueueLength() {
        return callExecutor.getReadQueueLength();
    }

    @Override
    public int getScanQueueLength() {
        return callExecutor.getScanQueueLength();
    }

    @Override
    public int getActiveWriteRpcHandlerCount() {
        return callExecutor.getActiveWriteHandlerCount();
    }

    @Override
    public int getActiveReadRpcHandlerCount() {
        return callExecutor.getActiveReadHandlerCount();
    }

    @Override
    public int getActiveScanRpcHandlerCount() {
        return callExecutor.getActiveScanHandlerCount();
    }

    @Override
    public CallQueueInfo getCallQueueInfo() {
        String queueName;

        CallQueueInfo callQueueInfo = new CallQueueInfo();

        if (null != callExecutor) {
            queueName = "Call Queue";
            callQueueInfo.setCallMethodCount(queueName, callExecutor.getCallQueueCountsSummary());
            callQueueInfo.setCallMethodSize(queueName, callExecutor.getCallQueueSizeSummary());
        }

        if (null != priorityExecutor) {
            queueName = "Priority Queue";
            callQueueInfo.setCallMethodCount(queueName, priorityExecutor.getCallQueueCountsSummary());
            callQueueInfo.setCallMethodSize(queueName, priorityExecutor.getCallQueueSizeSummary());
        }

        if (null != replicationExecutor) {
            queueName = "Replication Queue";
            callQueueInfo.setCallMethodCount(queueName, replicationExecutor.getCallQueueCountsSummary());
            callQueueInfo.setCallMethodSize(queueName, replicationExecutor.getCallQueueSizeSummary());
        }

        if (null != metaTransitionExecutor) {
            queueName = "Meta Transition Queue";
            callQueueInfo.setCallMethodCount(queueName,
                    metaTransitionExecutor.getCallQueueCountsSummary());
            callQueueInfo.setCallMethodSize(queueName, metaTransitionExecutor.getCallQueueSizeSummary());
        }

        return callQueueInfo;
    }

}

