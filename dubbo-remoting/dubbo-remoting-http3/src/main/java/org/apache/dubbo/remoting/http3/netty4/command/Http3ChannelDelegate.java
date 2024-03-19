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
package org.apache.dubbo.remoting.http3.netty4.command;

import org.apache.dubbo.remoting.http12.HttpMetadata;
import org.apache.dubbo.remoting.http12.HttpOutputMessage;
import org.apache.dubbo.remoting.http3.netty4.Http3OutputMessage;
import org.apache.dubbo.remoting.http3.netty4.Http3StreamChannel;

import java.net.SocketAddress;
import java.util.concurrent.CompletableFuture;

public class Http3ChannelDelegate implements Http3StreamChannel {

    private final Http3StreamChannel http3StreamChannel;

    public Http3ChannelDelegate(Http3StreamChannel http3StreamChannel) {
        this.http3StreamChannel = http3StreamChannel;
    }

    public Http3StreamChannel getHttp3StreamChannel() {
        return http3StreamChannel;
    }

    @Override
    public CompletableFuture<Void> writeHeader(HttpMetadata httpMetadata) {
        return http3StreamChannel.writeHeader(httpMetadata);
    }

    @Override
    public CompletableFuture<Void> writeMessage(HttpOutputMessage httpOutputMessage) {
        return http3StreamChannel.writeMessage(httpOutputMessage);
    }

    @Override
    public SocketAddress remoteAddress() {
        return http3StreamChannel.remoteAddress();
    }

    @Override
    public SocketAddress localAddress() {
        return http3StreamChannel.localAddress();
    }

    @Override
    public void flush() {
        http3StreamChannel.flush();
    }

    @Override
    public CompletableFuture<Void> writeResetFrame(long errorCode) {
        return http3StreamChannel.writeResetFrame(errorCode);
    }

    @Override
    public Http3OutputMessage newOutputMessage(boolean endStream) {
        return http3StreamChannel.newOutputMessage(endStream);
    }
}
