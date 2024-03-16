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

import io.netty.incubator.codec.http3.Http3ErrorCode;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

import org.apache.dubbo.rpc.protocol.tri.stream.h3.TripleHttp3StreamChannelFuture;

public class QuicCancelQueueCommand extends QuicStreamQueueCommand {
    private final Http3ErrorCode error;

    public QuicCancelQueueCommand(TripleHttp3StreamChannelFuture streamChannelFuture, Http3ErrorCode error) {
        super(streamChannelFuture);
        this.error = error;
    }

    public static QuicCancelQueueCommand createCommand(TripleHttp3StreamChannelFuture streamChannelFuture, Http3ErrorCode error) {
        return new QuicCancelQueueCommand(streamChannelFuture, error);
    }

    @Override
    public void doSend(ChannelHandlerContext ctx, ChannelPromise promise) {
        ctx.write(Http3ErrorCode.H3_REQUEST_CANCELLED, promise);
    }
}
