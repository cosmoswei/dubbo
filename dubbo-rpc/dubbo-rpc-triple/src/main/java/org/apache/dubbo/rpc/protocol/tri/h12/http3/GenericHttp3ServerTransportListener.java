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
package org.apache.dubbo.rpc.protocol.tri.h12.http3;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.threadpool.manager.ExecutorRepository;
import org.apache.dubbo.remoting.http12.HttpChannel;
import org.apache.dubbo.remoting.http12.exception.HttpStatusException;
import org.apache.dubbo.remoting.http12.h2.Http2InputMessage;
import org.apache.dubbo.remoting.http12.h2.Http2InputMessageFrame;
import org.apache.dubbo.remoting.http12.message.ListeningDecoder;
import org.apache.dubbo.remoting.http12.message.NoOpStreamingDecoder;
import org.apache.dubbo.remoting.http12.message.StreamingDecoder;
import org.apache.dubbo.remoting.http12.message.codec.JsonCodec;
import org.apache.dubbo.remoting.http3.netty4.Http3ServerChannelObserver;
import org.apache.dubbo.remoting.http3.netty4.Http3TransportListener;
import org.apache.dubbo.rpc.executor.ExecutorSupport;
import org.apache.dubbo.rpc.model.FrameworkModel;
import org.apache.dubbo.rpc.protocol.tri.h12.HttpMessageListener;
import org.apache.dubbo.rpc.protocol.tri.h12.ServerCallListener;

import java.io.ByteArrayInputStream;

import io.netty.incubator.codec.http3.Http3DataFrame;
import io.netty.incubator.codec.http3.Http3HeadersFrame;
import io.netty.incubator.codec.quic.QuicStreamChannel;

public class GenericHttp3ServerTransportListener
        extends Http3AbstractServerTransportListener<Http3HeadersFrame, Http3DataFrame>
        implements Http3TransportListener {

    private static final Http2InputMessage EMPTY_MESSAGE =
            new Http2InputMessageFrame(new ByteArrayInputStream(new byte[0]), true);

    private final ExecutorSupport executorSupport;
    private final StreamingDecoder streamingDecoder;
    private final Http3ServerCallToObserverAdapter serverChannelObserver;

    private ServerCallListener serverCallListener;

    public GenericHttp3ServerTransportListener(
            QuicStreamChannel quicStreamChannel, URL url, FrameworkModel frameworkModel) {
        super(frameworkModel, url, (HttpChannel) quicStreamChannel);
        executorSupport = ExecutorRepository.getInstance(url.getOrDefaultApplicationModel())
                .getExecutorSupport(url);
        streamingDecoder = newStreamingDecoder();
        serverChannelObserver = new Http3ServerCallToObserverAdapter(frameworkModel, quicStreamChannel);
        serverChannelObserver.setResponseEncoder(JsonCodec.INSTANCE);
        serverChannelObserver.setStreamingDecoder(streamingDecoder);
    }

    protected StreamingDecoder newStreamingDecoder() {
        // default no op
        return new NoOpStreamingDecoder();
    }

    @Override
    public void cancelByRemote(long errorCode) {
        serverChannelObserver.cancel(new HttpStatusException((int) errorCode));
        serverCallListener.onCancel(errorCode);
    }

    protected StreamingDecoder getStreamingDecoder() {
        return streamingDecoder;
    }

    protected final Http3ServerChannelObserver getServerChannelObserver() {
        return serverChannelObserver;
    }

    @Override
    public void onMetadata(Http3HeadersFrame metadata) {

    }

    @Override
    public void onData(Http3DataFrame http3DataFrame) {

    }

    private static class Http3StreamingDecodeListener implements ListeningDecoder.Listener {

        private final ServerCallListener serverCallListener;

        private Http3StreamingDecodeListener(ServerCallListener serverCallListener) {
            this.serverCallListener = serverCallListener;
        }

        @Override
        public void onMessage(Object message) {
            serverCallListener.onMessage(message);
        }

        @Override
        public void onClose() {
            serverCallListener.onComplete();
        }
    }
}
