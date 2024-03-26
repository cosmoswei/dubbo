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

import org.apache.dubbo.rpc.protocol.tri.command.QueuedCommand;
import org.apache.dubbo.rpc.protocol.tri.stream.h3.TripleHttp3StreamChannelFuture;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.incubator.codec.quic.QuicStreamChannel;
import io.netty.util.concurrent.Future;

public class QuicCreateStreamQueueCommand extends QueuedCommand {

    Future<QuicStreamChannel> quicStreamChannelFuture;

    private final TripleHttp3StreamChannelFuture http3StreamChannelFuture;

    private QuicCreateStreamQueueCommand(
            Future<QuicStreamChannel> future, TripleHttp3StreamChannelFuture http3StreamChannelFuture) {
        this.quicStreamChannelFuture = future;
        this.http3StreamChannelFuture = http3StreamChannelFuture;
        this.promise(http3StreamChannelFuture.getParentChannel()
                .newPromise());
        this.channel(http3StreamChannelFuture.getParentChannel());
    }

    public static QuicCreateStreamQueueCommand create(
            Future<QuicStreamChannel> future, TripleHttp3StreamChannelFuture streamChannelFuture) {
        return new QuicCreateStreamQueueCommand(future, streamChannelFuture);
    }

    @Override
    public void doSend(ChannelHandlerContext ctx, ChannelPromise promise) {
        // NOOP
    }

    @Override
    public void run(Channel channel) {
        // work in I/O thread
        Future<QuicStreamChannel> channelFuture = null;
        try {
            channelFuture = quicStreamChannelFuture.sync();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        if (channelFuture.isSuccess()) {
            http3StreamChannelFuture.complete(quicStreamChannelFuture.getNow());
        } else {
            http3StreamChannelFuture.completeExceptionally(channelFuture.cause());
        }
    }
}
