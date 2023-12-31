/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ LargeTests.class, ClientTests.class })
public class TestRestoreSnapshotFromClientWithRegionReplicas
    extends RestoreSnapshotFromClientTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRestoreSnapshotFromClientWithRegionReplicas.class);

  @Override
  protected int getNumReplicas() {
    return 3;
  }

  @Test
  public void testOnlineSnapshotAfterSplittingRegions() throws IOException, InterruptedException {
    List<RegionInfo> regionInfos = admin.getRegions(tableName);
    RegionReplicaUtil.removeNonDefaultRegions(regionInfos);

    // Split a region
    splitRegion(regionInfos.get(0));

    // Take a online snapshot
    admin.snapshot(snapshotName1, tableName);

    // Clone the snapshot to another table
    TableName clonedTableName =
      TableName.valueOf(name.getMethodName() + "-" + System.currentTimeMillis());
    admin.cloneSnapshot(snapshotName1, clonedTableName);

    verifyRowCount(TEST_UTIL, clonedTableName, snapshot1Rows);
  }
}
