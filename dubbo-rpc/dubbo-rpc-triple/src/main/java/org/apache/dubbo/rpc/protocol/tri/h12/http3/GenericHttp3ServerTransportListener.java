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
import org.apache.dubbo.common.threadpool.serial.SerializingExecutor;
import org.apache.dubbo.remoting.http12.HttpMethods;
import org.apache.dubbo.remoting.http12.exception.HttpStatusException;
import org.apache.dubbo.remoting.http12.message.DefaultListeningDecoder;
import org.apache.dubbo.remoting.http12.message.ListeningDecoder;
import org.apache.dubbo.remoting.http12.message.MethodMetadata;
import org.apache.dubbo.remoting.http12.message.NoOpStreamingDecoder;
import org.apache.dubbo.remoting.http12.message.StreamingDecoder;
import org.apache.dubbo.remoting.http12.message.codec.JsonCodec;
import org.apache.dubbo.remoting.http3.netty4.Http3Header;
import org.apache.dubbo.remoting.http3.netty4.Http3InputMessage;
import org.apache.dubbo.remoting.http3.netty4.Http3InputMessageFrame;
import org.apache.dubbo.remoting.http3.netty4.Http3ServerChannelObserver;
import org.apache.dubbo.remoting.http3.netty4.Http3StreamChannel;
import org.apache.dubbo.remoting.http3.netty4.Http3TransportListener;
import org.apache.dubbo.rpc.CancellationContext;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.executor.ExecutorSupport;
import org.apache.dubbo.rpc.model.FrameworkModel;
import org.apache.dubbo.rpc.model.MethodDescriptor;
import org.apache.dubbo.rpc.protocol.tri.ReflectionPackableMethod;
import org.apache.dubbo.rpc.protocol.tri.RpcInvocationBuildContext;
import org.apache.dubbo.rpc.protocol.tri.h12.AbstractServerTransportListener;
import org.apache.dubbo.rpc.protocol.tri.h12.BiStreamServerCallListener;
import org.apache.dubbo.rpc.protocol.tri.h12.HttpMessageListener;
import org.apache.dubbo.rpc.protocol.tri.h12.ServerCallListener;
import org.apache.dubbo.rpc.protocol.tri.h12.ServerStreamServerCallListener;
import org.apache.dubbo.rpc.protocol.tri.h12.UnaryServerCallListener;
import org.apache.dubbo.rpc.protocol.tri.h12.grpc.StreamingHttpMessageListener;

import java.io.ByteArrayInputStream;
import java.util.concurrent.Executor;

public class GenericHttp3ServerTransportListener extends AbstractServerTransportListener<Http3Header, Http3InputMessage>
        implements Http3TransportListener {

    private static final Http3InputMessage EMPTY_MESSAGE =
            new Http3InputMessageFrame(new ByteArrayInputStream(new byte[0]), true);

    private final ExecutorSupport executorSupport;
    private final StreamingDecoder streamingDecoder;
    private final Http3ServerCallToObserverAdapter serverChannelObserver;

    private ServerCallListener serverCallListener;

    public GenericHttp3ServerTransportListener(
            Http3StreamChannel http3StreamChannel, URL url, FrameworkModel frameworkModel) {
        super(frameworkModel, url, http3StreamChannel);
        executorSupport = ExecutorRepository.getInstance(url.getOrDefaultApplicationModel())
                .getExecutorSupport(url);
        streamingDecoder = newStreamingDecoder();
        serverChannelObserver = new Http3ServerCallToObserverAdapter(frameworkModel, http3StreamChannel);
        serverChannelObserver.setResponseEncoder(JsonCodec.INSTANCE);
        serverChannelObserver.setStreamingDecoder(streamingDecoder);
    }

    protected StreamingDecoder newStreamingDecoder() {
        // default no op
        return new NoOpStreamingDecoder();
    }

    @Override
    protected Executor initializeExecutor(Http3Header metadata) {
        return new SerializingExecutor(executorSupport.getExecutor(metadata));
    }

    @Override
    public void cancelByRemote(long errorCode) {
        serverChannelObserver.cancel(new HttpStatusException((int) errorCode));
        serverCallListener.onCancel(errorCode);
    }

    protected void doOnMetadata(Http3Header metadata) {
        if (metadata.isEndStream()) {
            if (!HttpMethods.supportBody(metadata.method())) {
                super.doOnMetadata(metadata);
                doOnData(EMPTY_MESSAGE);
            }
            return;
        }
        super.doOnMetadata(metadata);
    }

    @Override
    protected void onDataCompletion(Http3InputMessage message) {
        if (message.isEndStream()) {
            serverCallListener.onComplete();
        }
    }

    @Override
    protected HttpMessageListener buildHttpMessageListener() {
        RpcInvocationBuildContext context = getContext();
        RpcInvocation rpcInvocation = buildRpcInvocation(context);

        serverCallListener = startListener(rpcInvocation, context.getMethodDescriptor(), context.getInvoker());

        DefaultListeningDecoder listeningDecoder = new DefaultListeningDecoder(context.getHttpMessageDecoder(),
                context.getMethodMetadata()
                .getActualRequestTypes());
        listeningDecoder.setListener(new Http3StreamingDecodeListener(serverCallListener));
        streamingDecoder.setFragmentListener(new StreamingDecoder.DefaultFragmentListener(listeningDecoder));
        getServerChannelObserver().setStreamingDecoder(streamingDecoder);
        return new StreamingHttpMessageListener(streamingDecoder);
    }

    private ServerCallListener startListener(
            RpcInvocation invocation, MethodDescriptor methodDescriptor, Invoker<?> invoker) {
        Http3ServerChannelObserver responseObserver = getServerChannelObserver();
        CancellationContext cancellationContext = RpcContext.getCancellationContext();
        responseObserver.setCancellationContext(cancellationContext);
        switch (methodDescriptor.getRpcType()) {
            case UNARY:
                boolean applyCustomizeException = false;
                if (!getContext().isHasStub()) {
                    MethodMetadata methodMetadata = getContext().getMethodMetadata();
                    applyCustomizeException = ReflectionPackableMethod.needWrap(methodDescriptor,
                            methodMetadata.getActualRequestTypes(), methodMetadata.getActualResponseType());
                }
                UnaryServerCallListener unaryServerCallListener = startUnary(invocation, invoker, responseObserver);
                unaryServerCallListener.setApplyCustomizeException(applyCustomizeException);
                return unaryServerCallListener;
            case SERVER_STREAM:
                return startServerStreaming(invocation, invoker, responseObserver);
            case BI_STREAM:
            case CLIENT_STREAM:
                return startBiStreaming(invocation, invoker, responseObserver);
            default:
                throw new IllegalStateException("Can not reach here");
        }
    }

    private UnaryServerCallListener startUnary(
            RpcInvocation invocation, Invoker<?> invoker, Http3ServerChannelObserver responseObserver) {
        return new UnaryServerCallListener(invocation, invoker, responseObserver);
    }

    private ServerStreamServerCallListener startServerStreaming(
            RpcInvocation invocation, Invoker<?> invoker, Http3ServerChannelObserver responseObserver) {
        return new ServerStreamServerCallListener(invocation, invoker, responseObserver);
    }

    private BiStreamServerCallListener startBiStreaming(
            RpcInvocation invocation, Invoker<?> invoker, Http3ServerChannelObserver responseObserver) {
        return new BiStreamServerCallListener(invocation, invoker, responseObserver);
    }

    protected final Http3ServerChannelObserver getServerChannelObserver() {
        return serverChannelObserver;
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
