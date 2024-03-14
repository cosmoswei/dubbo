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
package org.apache.dubbo.rpc.protocol.tri.stream.h3;

import org.apache.dubbo.common.utils.Assert;

import java.util.concurrent.CompletableFuture;

import io.netty.channel.Channel;
import io.netty.incubator.codec.quic.QuicStreamChannel;

public class TripleHttp3StreamChannelFuture extends CompletableFuture<QuicStreamChannel> {

    private final Channel parentChannel;

    private Throwable cause;

    public TripleHttp3StreamChannelFuture(Channel parentChannel) {
        Assert.notNull(parentChannel, "parentChannel cannot be null.");
        this.parentChannel = parentChannel;
    }

    public TripleHttp3StreamChannelFuture(QuicStreamChannel channel) {
        this.complete(channel);
        this.parentChannel = channel.parent();
    }

    public Channel getParentChannel() {
        return parentChannel;
    }

    @Override
    public boolean completeExceptionally(Throwable cause) {
        boolean result = super.completeExceptionally(cause);
        if (result) {
            this.cause = cause;
        }
        return result;
    }

    public Throwable cause() {
        return cause;
    }

    public boolean isSuccess() {
        return isDone() && cause() == null;
    }

    public QuicStreamChannel getNow() {
        return getNow(null);
    }
}
