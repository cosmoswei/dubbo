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

import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.remoting.http12.HttpStatus;
import org.apache.dubbo.remoting.http12.exception.HttpStatusException;
import org.apache.dubbo.remoting.http12.netty4.h2.NettyHttp2FrameHandler;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http2.Http2ResetFrame;

import static org.apache.dubbo.common.constants.LoggerCodeConstants.PROTOCOL_FAILED_RESPONSE;

public class NettyHttp3FrameHandler extends ChannelDuplexHandler {

    private static final ErrorTypeAwareLogger LOGGER =
            LoggerFactory.getErrorTypeAwareLogger(NettyHttp2FrameHandler.class);

    private final Http3StreamChannel http3StreamChannel;

    private final Http3TransportListener transportListener;

    public NettyHttp3FrameHandler(Http3StreamChannel http3StreamChannel, Http3TransportListener transportListener) {
        this.http3StreamChannel = http3StreamChannel;
        this.transportListener = transportListener;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof Http3Header) {
            transportListener.onMetadata((Http3Header) msg);
        } else if (msg instanceof Http3InputMessage) {
            transportListener.onData((Http3InputMessage) msg);
        } else {
            super.channelRead(ctx, msg);
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        // reset frame
        if (evt instanceof Http2ResetFrame) {
            long errorCode = ((Http2ResetFrame) evt).errorCode();
            transportListener.cancelByRemote(errorCode);
        } else {
            super.userEventTriggered(ctx, evt);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        if (LOGGER.isWarnEnabled()) {
            LOGGER.warn(PROTOCOL_FAILED_RESPONSE, "", "", "Exception in processing triple message", cause);
        }
        int statusCode = HttpStatus.INTERNAL_SERVER_ERROR.getCode();
        if (cause instanceof HttpStatusException) {
            statusCode = ((HttpStatusException) cause).getStatusCode();
        }
        http3StreamChannel.writeResetFrame(statusCode);
    }
}
