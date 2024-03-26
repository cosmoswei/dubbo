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
package org.apache.dubbo.rpc.protocol.tri.transport.h3;

import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.rpc.TriRpcStatus;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http2.Http2Error;
import io.netty.handler.codec.http2.Http2GoAwayFrame;
import io.netty.handler.codec.http2.Http2ResetFrame;
import io.netty.handler.codec.http2.Http2StreamFrame;
import io.netty.incubator.codec.http3.Http3DataFrame;
import io.netty.incubator.codec.http3.Http3GoAwayFrame;
import io.netty.incubator.codec.http3.Http3HeadersFrame;

import static org.apache.dubbo.common.constants.LoggerCodeConstants.PROTOCOL_FAILED_SERIALIZE_TRIPLE;

public final class TripleHttp3ClientResponseHandler extends SimpleChannelInboundHandler<Http2StreamFrame> {

    private static final ErrorTypeAwareLogger LOGGER =
            LoggerFactory.getErrorTypeAwareLogger(TripleHttp3ClientResponseHandler.class);
    private final Http3TransportListener transportListener;

    public TripleHttp3ClientResponseHandler(Http3TransportListener listener) {
        super(false);
        this.transportListener = listener;
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        super.userEventTriggered(ctx, evt);
        if (evt instanceof Http2GoAwayFrame) {
            Http3GoAwayFrame event = (Http3GoAwayFrame) evt;
            ctx.close();
            LOGGER.debug("Event triggered, event type is: " + event.type() + ", last stream id is: " + event.id());
        } else if (evt instanceof Http2ResetFrame) {
            onResetRead(ctx, (Http2ResetFrame) evt);
        }
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Http2StreamFrame msg) throws Exception {
        if (msg instanceof Http3HeadersFrame) {
            final Http3HeadersFrame headers = (Http3HeadersFrame) msg;
            transportListener.onHeader(headers.headers(), headers.type() == 1);
        } else if (msg instanceof Http3DataFrame) {
            final Http3DataFrame data = (Http3DataFrame) msg;
            transportListener.onData(data.content(), data.type() == 1);
        } else {
            super.channelRead(ctx, msg);
        }
    }

    private void onResetRead(ChannelHandlerContext ctx, Http2ResetFrame resetFrame) {
        LOGGER.warn(PROTOCOL_FAILED_SERIALIZE_TRIPLE, "", "",
                "Triple Client received remote reset errorCode=" + resetFrame.errorCode());
        transportListener.cancelByRemote(resetFrame.errorCode());
        ctx.close();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        ctx.close();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        final TriRpcStatus status = TriRpcStatus.INTERNAL.withCause(cause);
        LOGGER.warn(PROTOCOL_FAILED_SERIALIZE_TRIPLE, "", "",
                "Meet Exception on ClientResponseHandler, status code is: " + status.code, cause);
        transportListener.cancelByRemote(Http2Error.INTERNAL_ERROR.code());
        ctx.close();
    }
}