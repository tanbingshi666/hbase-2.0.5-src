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
package org.apache.hadoop.hbase.wal;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.hbase.thirdparty.io.netty.channel.EventLoopGroup;

/**
 * Helper class for passing netty event loop config to {@link AsyncFSWALProvider}.
 * @since 2.0.0
 */
@InterfaceAudience.Private
public final class NettyAsyncFSWALConfigHelper {

    private static final String EVENT_LOOP_CONFIG = "hbase.wal.async.event-loop.config";

    private static final String CONFIG_NAME = "global-event-loop";

    private static final Map<String, Pair<EventLoopGroup, Class<? extends Channel>>> EVENT_LOOP_CONFIG_MAP =
            new HashMap<>();

    /**
     * Set the EventLoopGroup and channel class for {@code AsyncFSWALProvider}.
     */
    public static void setEventLoopConfig(Configuration conf, EventLoopGroup group,
                                          Class<? extends Channel> channelClass) {
        // this
        Preconditions.checkNotNull(group, "group is null");
        Preconditions.checkNotNull(channelClass, "channel class is null");
        conf.set(EVENT_LOOP_CONFIG, CONFIG_NAME);
        EVENT_LOOP_CONFIG_MAP.put(CONFIG_NAME,
                Pair.<EventLoopGroup, Class<? extends Channel>>newPair(group, channelClass));
    }

    static Pair<EventLoopGroup, Class<? extends Channel>> getEventLoopConfig(Configuration conf) {
        String name = conf.get(EVENT_LOOP_CONFIG);
        if (StringUtils.isBlank(name)) {
            return null;
        }
        return EVENT_LOOP_CONFIG_MAP.get(name);
    }

    private NettyAsyncFSWALConfigHelper() {
    }
}
