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
package org.apache.dubbo.remoting.http3.netty4;

import org.apache.dubbo.remoting.http12.HttpMetadata;
import org.apache.dubbo.remoting.http12.HttpOutputMessage;
import org.apache.dubbo.remoting.http12.netty4.NettyHttpChannelFutureListener;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.CompletableFuture;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.handler.codec.http2.DefaultHttp2ResetFrame;
import io.netty.incubator.codec.http3.DefaultHttp3DataFrame;
import io.netty.incubator.codec.http3.DefaultHttp3HeadersFrame;
import io.netty.incubator.codec.quic.QuicStreamAddress;
import io.netty.incubator.codec.quic.QuicStreamChannel;

public class NettyHttp3StreamChannel implements Http3StreamChannel {

    private final QuicStreamChannel quicStreamChannel;

    public NettyHttp3StreamChannel(QuicStreamChannel quicStreamChannel) {
        this.quicStreamChannel = quicStreamChannel;
    }

    @Override
    public CompletableFuture<Void> writeHeader(HttpMetadata httpMetadata) {
        // WriteQueue.enqueue header frame
        NettyHttpChannelFutureListener nettyHttpChannelFutureListener = new NettyHttpChannelFutureListener();
        DefaultHttp3HeadersFrame frame = new DefaultHttp3HeadersFrame();
        httpMetadata.headers()
                .forEach((key, value) -> frame.headers()
                        .add(key, value));

        quicStreamChannel.write(frame)
                .addListener(nettyHttpChannelFutureListener);
        return nettyHttpChannelFutureListener;
    }

    @Override
    public CompletableFuture<Void> writeMessage(HttpOutputMessage httpOutputMessage) {
        NettyHttpChannelFutureListener nettyHttpChannelFutureListener = new NettyHttpChannelFutureListener();
        ByteBufOutputStream body = (ByteBufOutputStream) httpOutputMessage.getBody();
        DefaultHttp3DataFrame frame = new DefaultHttp3DataFrame(body.buffer());
        quicStreamChannel.write(frame)
                .addListener(nettyHttpChannelFutureListener);
        return nettyHttpChannelFutureListener;
    }

    @Override
    public Http3OutputMessage newOutputMessage(boolean endStream) {
        ByteBuf buffer = quicStreamChannel.alloc()
                .buffer();
        ByteBufOutputStream outputStream = new ByteBufOutputStream(buffer);
        return new Http3OutputMessageFrame(outputStream, endStream);
    }

    @Override
    public SocketAddress remoteAddress() {
        // TODO return the actual remote address
        QuicStreamAddress quicStreamAddress = this.quicStreamChannel.remoteAddress();
        InetSocketAddress inetSocketAddress = new InetSocketAddress(1001);
        return inetSocketAddress;
    }

    @Override
    public SocketAddress localAddress() {
        return this.quicStreamChannel.localAddress();
    }

    @Override
    public void flush() {
        this.quicStreamChannel.flush();
    }

    @Override
    public CompletableFuture<Void> writeResetFrame(long errorCode) {
        DefaultHttp2ResetFrame resetFrame = new DefaultHttp2ResetFrame(errorCode);
        NettyHttpChannelFutureListener nettyHttpChannelFutureListener = new NettyHttpChannelFutureListener();
        quicStreamChannel.write(resetFrame)
                .addListener(nettyHttpChannelFutureListener);
        return nettyHttpChannelFutureListener;
    }
}
