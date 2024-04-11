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
package org.apache.dubbo.rpc.protocol.tri;

import io.netty.channel.ChannelHandlerContext;
import io.netty.incubator.codec.http3.Http3DataFrame;
import io.netty.incubator.codec.http3.Http3HeadersFrame;
import io.netty.incubator.codec.http3.Http3RequestStreamInboundHandler;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.remoting.api.AbstractWireProtocol;
import org.apache.dubbo.remoting.api.ProtocolDetector;
import org.apache.dubbo.remoting.api.pu.ChannelHandlerPretender;
import org.apache.dubbo.remoting.api.pu.ChannelOperator;
import org.apache.dubbo.remoting.api.ssl.ContextOperator;
import org.apache.dubbo.remoting.http12.netty4.HttpWriteQueueHandler;
import org.apache.dubbo.remoting.http3.netty4.NettyHttp3FrameCodec;
import org.apache.dubbo.remoting.http3.netty4.NettyHttp3ProtocolSelectorHandler;
import org.apache.dubbo.rpc.model.FrameworkModel;
import org.apache.dubbo.rpc.model.ScopeModelAware;
import org.apache.dubbo.rpc.protocol.tri.h12.TripleProtocolDetector;
import org.apache.dubbo.rpc.protocol.tri.h12.http3.GenericHttp3ServerTransportListenerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.netty.incubator.codec.http3.Http3;
import io.netty.incubator.codec.http3.Http3ServerConnectionHandler;
import io.netty.incubator.codec.quic.InsecureQuicTokenHandler;
import io.netty.incubator.codec.quic.QuicSslContext;
import io.netty.incubator.codec.quic.QuicSslContextBuilder;
import io.netty.incubator.codec.quic.QuicStreamChannel;

public class TripleHttp3Protocol extends AbstractWireProtocol implements ScopeModelAware {

    private FrameworkModel frameworkModel;

    public TripleHttp3Protocol() {
        super(new TripleProtocolDetector());
    }

    @Override
    public void configClientPipeline(URL url, ChannelOperator operator, ContextOperator contextOperator) {
        QuicSslContext context = QuicSslContextBuilder.forClient()
                .trustManager(InsecureTrustManagerFactory.INSTANCE)
                .applicationProtocols(Http3.supportedApplicationProtocols())
                .build();

        ChannelHandler codec = Http3.newQuicClientCodecBuilder()
                .sslContext(context)
                .maxIdleTimeout(5000, TimeUnit.MILLISECONDS)
                .initialMaxData(10000000)
                .initialMaxStreamDataBidirectionalLocal(1000000)
                .build();

        List<org.apache.dubbo.remoting.ChannelHandler> handlers = new ArrayList<>();
        handlers.add(new ChannelHandlerPretender(codec));
        operator.configChannelHandler(handlers);
    }

    @Override
    public void configServerProtocolHandler(URL url, ChannelOperator operator) {
        try {
            SelfSignedCertificate cert = new SelfSignedCertificate();
            QuicSslContext sslContext = QuicSslContextBuilder.forServer(cert.key(), null, cert.cert())
                    .applicationProtocols(Http3.supportedApplicationProtocols())
                    .build();
            ChannelHandler codec = Http3.newQuicServerCodecBuilder()
                    .sslContext(sslContext)
                    .maxIdleTimeout(5000, TimeUnit.MILLISECONDS)
                    .initialMaxData(10000000)
                    .initialMaxStreamDataBidirectionalLocal(1000000)
                    .initialMaxStreamDataBidirectionalRemote(1000000)
                    .initialMaxStreamsBidirectional(100)
                    .tokenHandler(InsecureQuicTokenHandler.INSTANCE)
                    .handler(new Http3ServerConnectionHandler(new ChannelInitializer<QuicStreamChannel>() {
                        @Override
                        protected void initChannel(QuicStreamChannel ch) {
                            ch.pipeline()
                                    .addLast(new NettyHttp3FrameCodec());
                            ch.pipeline()
                                    .addLast(new HttpWriteQueueHandler());
                            ch.pipeline()
                                    .addLast(new NettyHttp3ProtocolSelectorHandler(url, frameworkModel,
                                            GenericHttp3ServerTransportListenerFactory.INSTANCE));
//                            ch.pipeline()
//                                    .addLast(new Http3RequestStreamInboundHandler() {
//                                        @Override
//                                        protected void channelRead(
//                                                ChannelHandlerContext channelHandlerContext,
//                                                Http3HeadersFrame http3HeadersFrame) throws Exception {
//
//                                        }
//
//                                        @Override
//                                        protected void channelRead(
//                                                ChannelHandlerContext channelHandlerContext,
//                                                Http3DataFrame http3DataFrame) throws Exception {
//
//                                        }
//
//                                        @Override
//                                        protected void channelInputClosed(ChannelHandlerContext channelHandlerContext) throws Exception {
//
//                                        }
//                                    });
                        }
                    }))
                    .build();

            List<org.apache.dubbo.remoting.ChannelHandler> handlers = new ArrayList<>();
            handlers.add(new ChannelHandlerPretender(codec));
            operator.configChannelHandler(handlers);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void setFrameworkModel(FrameworkModel frameworkModel) {
        this.frameworkModel = frameworkModel;
    }
}
