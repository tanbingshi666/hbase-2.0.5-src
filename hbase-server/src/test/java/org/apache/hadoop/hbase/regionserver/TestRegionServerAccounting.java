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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.io.util.MemorySizeUtil;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestRegionServerAccounting {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRegionServerAccounting.class);

  @Test
  public void testOnheapMemstoreHigherWaterMarkLimits() {
    Configuration conf = HBaseConfiguration.create();
    conf.setFloat(MemorySizeUtil.MEMSTORE_SIZE_KEY, 0.2f);
    // try for default cases
    RegionServerAccounting regionServerAccounting = new RegionServerAccounting(conf);
    MemStoreSize memstoreSize =
        new MemStoreSize((3L * 1024L * 1024L * 1024L), (1L * 1024L * 1024L * 1024L), 0, 0);
    regionServerAccounting.incGlobalMemStoreSize(memstoreSize);
    assertEquals(FlushType.ABOVE_ONHEAP_HIGHER_MARK, regionServerAccounting.isAboveHighWaterMark());
  }

  @Test
  public void testOnheapMemstoreLowerWaterMarkLimits() {
    Configuration conf = HBaseConfiguration.create();
    conf.setFloat(MemorySizeUtil.MEMSTORE_SIZE_KEY, 0.2f);
    // try for default cases
    RegionServerAccounting regionServerAccounting = new RegionServerAccounting(conf);
    MemStoreSize memstoreSize =
        new MemStoreSize((3L * 1024L * 1024L * 1024L), (1L * 1024L * 1024L * 1024L), 0, 0);
    regionServerAccounting.incGlobalMemStoreSize(memstoreSize);
    assertEquals(FlushType.ABOVE_ONHEAP_LOWER_MARK, regionServerAccounting.isAboveLowWaterMark());
  }

  @Test
  public void testOffheapMemstoreHigherWaterMarkLimitsDueToDataSize() {
    Configuration conf = HBaseConfiguration.create();
    // setting 1G as offheap data size
    conf.setLong(MemorySizeUtil.OFFHEAP_MEMSTORE_SIZE_KEY, (1L * 1024L));
    // try for default cases
    RegionServerAccounting regionServerAccounting = new RegionServerAccounting(conf);
    // this will breach offheap limit as data size is higher and not due to heap size
    MemStoreSize memstoreSize =
        new MemStoreSize((3L * 1024L * 1024L * 1024L), 0, (1L * 1024L * 1024L * 1024L), 100);
    regionServerAccounting.incGlobalMemStoreSize(memstoreSize);
    assertEquals(FlushType.ABOVE_OFFHEAP_HIGHER_MARK,
      regionServerAccounting.isAboveHighWaterMark());
  }

  @Test
  public void testOffheapMemstoreHigherWaterMarkLimitsDueToHeapSize() {
    Configuration conf = HBaseConfiguration.create();
    conf.setFloat(MemorySizeUtil.MEMSTORE_SIZE_KEY, 0.2f);
    // setting 1G as offheap data size
    conf.setLong(MemorySizeUtil.OFFHEAP_MEMSTORE_SIZE_KEY, (1L * 1024L));
    // try for default cases
    RegionServerAccounting regionServerAccounting = new RegionServerAccounting(conf);
    // this will breach higher limit as heap size is higher and not due to offheap size
    MemStoreSize memstoreSize =
        new MemStoreSize((3L * 1024L * 1024L), (2L * 1024L * 1024L * 1024L), 0, 0);
    regionServerAccounting.incGlobalMemStoreSize(memstoreSize);
    assertEquals(FlushType.ABOVE_ONHEAP_HIGHER_MARK, regionServerAccounting.isAboveHighWaterMark());
  }

  @Test
  public void testOffheapMemstoreLowerWaterMarkLimitsDueToDataSize() {
    Configuration conf = HBaseConfiguration.create();
    // setting 1G as offheap data size
    conf.setLong(MemorySizeUtil.OFFHEAP_MEMSTORE_SIZE_KEY, (1L * 1024L));
    // try for default cases
    RegionServerAccounting regionServerAccounting = new RegionServerAccounting(conf);
    // this will breach offheap limit as data size is higher and not due to heap size
    MemStoreSize memstoreSize =
        new MemStoreSize((3L * 1024L * 1024L * 1024L), 0, (1L * 1024L * 1024L * 1024L), 100);
    regionServerAccounting.incGlobalMemStoreSize(memstoreSize);
    assertEquals(FlushType.ABOVE_OFFHEAP_LOWER_MARK, regionServerAccounting.isAboveLowWaterMark());
  }

  @Test
  public void testOffheapMemstoreLowerWaterMarkLimitsDueToHeapSize() {
    Configuration conf = HBaseConfiguration.create();
    conf.setFloat(MemorySizeUtil.MEMSTORE_SIZE_KEY, 0.2f);
    // setting 1G as offheap data size
    conf.setLong(MemorySizeUtil.OFFHEAP_MEMSTORE_SIZE_KEY, (1L * 1024L));
    // try for default cases
    RegionServerAccounting regionServerAccounting = new RegionServerAccounting(conf);
    // this will breach higher limit as heap size is higher and not due to offheap size
    MemStoreSize memstoreSize =
        new MemStoreSize((3L * 1024L * 1024L), (2L * 1024L * 1024L * 1024L), 0, 0);
    regionServerAccounting.incGlobalMemStoreSize(memstoreSize);
    assertEquals(FlushType.ABOVE_ONHEAP_LOWER_MARK, regionServerAccounting.isAboveLowWaterMark());
  }
}
