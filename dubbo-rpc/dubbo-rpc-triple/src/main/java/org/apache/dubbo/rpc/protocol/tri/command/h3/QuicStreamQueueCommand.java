/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.rpc.protocol.tri.command.h3;

import org.apache.dubbo.common.utils.Assert;
import org.apache.dubbo.rpc.protocol.tri.command.QueuedCommand;
import org.apache.dubbo.rpc.protocol.tri.stream.h3.TripleHttp3StreamChannelFuture;

import io.netty.channel.Channel;

public abstract class QuicStreamQueueCommand extends QueuedCommand {

    protected final TripleHttp3StreamChannelFuture http3StreamChannelFuture;

    protected QuicStreamQueueCommand(TripleHttp3StreamChannelFuture http3StreamChannelFuture) {
        Assert.notNull(http3StreamChannelFuture, "http3StreamChannelFuture cannot be null.");
        this.http3StreamChannelFuture = http3StreamChannelFuture;
        this.promise(http3StreamChannelFuture.getParentChannel()
                .newPromise());
    }

    @Override
    public void run(Channel channel) {
        if (http3StreamChannelFuture.isSuccess()) {
            super.run(channel);
            return;
        }
        promise().setFailure(http3StreamChannelFuture.cause());
    }

    @Override
    public Channel channel() {
        return this.http3StreamChannelFuture.getNow();
    }
}
