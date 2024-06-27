package org.apache.dubbo.rpc.protocol.tri.stream.h3;

import io.netty.incubator.codec.http3.Http3DataFrame;
import io.netty.incubator.codec.http3.Http3HeadersFrame;
import io.netty.incubator.codec.http3.Http3RequestStreamInboundHandler;

import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.JsonUtils;
import org.apache.dubbo.rpc.TriRpcStatus;
import org.apache.dubbo.rpc.model.FrameworkModel;
import org.apache.dubbo.rpc.protocol.tri.ClassLoadUtil;
import org.apache.dubbo.rpc.protocol.tri.ExceptionUtils;
import org.apache.dubbo.rpc.protocol.tri.TripleConstant;
import org.apache.dubbo.rpc.protocol.tri.TripleHeaderEnum;
import org.apache.dubbo.rpc.protocol.tri.command.h3.QuicCancelQueueCommand;
import org.apache.dubbo.rpc.protocol.tri.command.h3.QuicCreateStreamQueueCommand;
import org.apache.dubbo.rpc.protocol.tri.command.h3.QuicDataQueueCommand;
import org.apache.dubbo.rpc.protocol.tri.command.h3.QuicHeaderQueueCommand;
import org.apache.dubbo.rpc.protocol.tri.compressor.DeCompressor;
import org.apache.dubbo.rpc.protocol.tri.compressor.Identity;
import org.apache.dubbo.rpc.protocol.tri.frame.Deframer;
import org.apache.dubbo.rpc.protocol.tri.frame.TriDecoder;
import org.apache.dubbo.rpc.protocol.tri.stream.AbstractStream;
import org.apache.dubbo.rpc.protocol.tri.stream.ClientStream;
import org.apache.dubbo.rpc.protocol.tri.stream.StreamUtils;
import org.apache.dubbo.rpc.protocol.tri.transport.TripleCommandOutBoundHandler;
import org.apache.dubbo.rpc.protocol.tri.transport.TripleWriteQueue;
import org.apache.dubbo.rpc.protocol.tri.transport.h3.Http3TransportListener;
import org.apache.dubbo.rpc.protocol.tri.transport.h3.TripleHttp3ClientResponseHandler;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

import com.google.protobuf.Any;
import com.google.rpc.DebugInfo;
import com.google.rpc.ErrorInfo;
import com.google.rpc.Status;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.incubator.codec.http3.Http3;
import io.netty.incubator.codec.http3.Http3ErrorCode;
import io.netty.incubator.codec.http3.Http3Headers;
import io.netty.incubator.codec.quic.QuicChannel;
import io.netty.incubator.codec.quic.QuicStreamChannel;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.Future;

import static org.apache.dubbo.common.constants.LoggerCodeConstants.INTERNAL_ERROR;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.PROTOCOL_FAILED_PARSE;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.PROTOCOL_FAILED_RESPONSE;

public class TripleHttp3ClientStream extends AbstractStream implements ClientStream {

    private static final ErrorTypeAwareLogger LOGGER =
            LoggerFactory.getErrorTypeAwareLogger(TripleHttp3ClientStream.class);

    // 这个是流状态的监听器，区别与流响应的监听器，对应的是 ObserverToClientCallListenerAdapter
    public final ClientStream.Listener listener;
    // Triple 协议的写入队列，不能直接ChannelRead.write()
    private final TripleWriteQueue writeQueue;
    // 解码帧
    private Deframer deframer;
    // 用于异步回调的Future
    private final TripleHttp3StreamChannelFuture http3StreamChannelFuture;
    // 半关闭标志
    private boolean halfClosed;
    // 重置标志
    private boolean rst;
    // 返回异常标志
    private boolean isReturnTriException = false;
    // 父 parentChannel
    private final QuicChannel parentChannel;

    public TripleHttp3ClientStream(
            FrameworkModel frameworkModel,
            Executor executor,
            ClientStream.Listener listener,
            QuicChannel parentChannel,
            TripleWriteQueue writeQueue) {
        super(executor, frameworkModel);
        this.listener = listener;
        this.writeQueue = writeQueue;
        this.parentChannel = parentChannel;
        this.http3StreamChannelFuture = initTripleHttp3StreamChannelFuture();
    }

    private TripleHttp3StreamChannelFuture initTripleHttp3StreamChannelFuture() {
        TripleHttp3StreamChannelFuture future = new TripleHttp3StreamChannelFuture(parentChannel);
        Future<QuicStreamChannel> quicStreamChannelFuture;
        quicStreamChannelFuture = Http3.newRequestStream(parentChannel, new Http3RequestStreamInboundHandler() {
            @Override
            protected void channelRead(
                    ChannelHandlerContext channelHandlerContext,
                    Http3HeadersFrame http3HeadersFrame) throws Exception {
                // 客户端从这里拿返回结果头，写到writeQueue中去？
            }

            @Override
            protected void channelRead(
                    ChannelHandlerContext channelHandlerContext,
                    Http3DataFrame http3DataFrame) throws Exception {
                // 客户端从这里拿返回结果Body
                // 也可以交给其他handler
            }

            @Override
            protected void channelInputClosed(ChannelHandlerContext channelHandlerContext) throws Exception {

            }

            @Override
            public void handlerAdded(ChannelHandlerContext ctx) {
                Channel channel = ctx.channel();
                channel.pipeline()
                        .addLast(new TripleCommandOutBoundHandler());
                // 这里是响应回调器
                channel.pipeline()
                        .addLast(new TripleHttp3ClientResponseHandler(createTransportListener()));
            }
        });
        // 这是写请求的队列工具数据结构
        QuicCreateStreamQueueCommand cmd = QuicCreateStreamQueueCommand.create(quicStreamChannelFuture,
                future);
        this.writeQueue.enqueue(cmd);
        return future;
    }

    private Http3TransportListener createTransportListener() {
        return new Http3ClientTransportListener();
    }

    // 发送请求，异步返回
    public Future<?> sendMessage(byte[] message, int compressFlag, boolean eos) {
        final QuicDataQueueCommand cmd = QuicDataQueueCommand.create(http3StreamChannelFuture, message, false,
                compressFlag);

        return this.writeQueue.enqueueFuture(cmd, parentChannel.eventLoop())
                .addListener(future -> {
                    if (!future.isSuccess()) {
                        cancelByLocal(TriRpcStatus.INTERNAL.withDescription("Client write message failed")
                                .withCause(future.cause()));
                        transportException(future.cause());
                    }
                });
    }

    private void transportException(Throwable cause) {
        final TriRpcStatus status = TriRpcStatus.INTERNAL.withDescription("Http3 exception")
                .withCause(cause);
        listener.onComplete(status, null, null, false);
    }

    public Future<?> halfClose() {
        return null;
    }

    public Future<?> sendHeader(Http3Headers headers) {
        if (this.writeQueue == null) {
            // already processed at createStream()
            return parentChannel.newFailedFuture(new IllegalStateException("Stream already closed"));
        }
        ChannelFuture checkResult = preCheck();
        if (!checkResult.isSuccess()) {
            return checkResult;
        }
        final QuicHeaderQueueCommand headerCmd = QuicHeaderQueueCommand.createHeaders(http3StreamChannelFuture,
                headers);
        return writeQueue.enqueueFuture(headerCmd, parentChannel.eventLoop())
                .addListener(future -> {
                    if (!future.isSuccess()) {
                        transportException(future.cause());
                    }
                });
    }

    private ChannelFuture preCheck() {
        if (rst) {
            return http3StreamChannelFuture.getNow()
                    .newFailedFuture(new IOException("stream channel has reset"));
        }
        return parentChannel.newSucceededFuture();
    }

    @Override
    public Future<?> sendHeader(Http2Headers headers) {
        return null;
    }

    public Future<?> cancelByLocal(TriRpcStatus status) {
        return null;
    }

    public SocketAddress remoteAddress() {
        return null;
    }

    public void request(int n) {

    }

    class Http3ClientTransportListener implements Http3TransportListener {

        private TriRpcStatus transportError;
        private DeCompressor decompressor;
        private boolean headerReceived;
        private Http3Headers trailers;

        @Override
        public void onHeader(Http3Headers headers, boolean endStream) {
            executor.execute(() -> {
                if (endStream) {
                    if (!halfClosed) {
                        QuicStreamChannel channel = http3StreamChannelFuture.getNow();
                        if (channel.isActive() && !rst) {
                            writeQueue.enqueue(QuicCancelQueueCommand.createCommand(http3StreamChannelFuture,
                                    Http3ErrorCode.H3_REQUEST_CANCELLED));
                            rst = true;
                        }
                    }
                    onTrailersReceived(headers);
                } else {
                    onHeaderReceived(headers);
                }
            });
        }

        // 需要重写
        void onTrailersReceived(Http3Headers trailers) {
            if (transportError == null && !headerReceived) {
                transportError = validateHeaderStatus(trailers);
            }
            if (transportError != null) {
                transportError = transportError.appendDescription("trailers: " + trailers);
            } else {
                this.trailers = trailers;
                TriRpcStatus status = statusFromTrailers(trailers);
                if (deframer == null) {
                    finishProcess(status, trailers, false);
                }
                if (deframer != null) {
                    deframer.close();
                }
            }
        }

        void onHeaderReceived(Http3Headers headers) {
            if (transportError != null) {
                transportError.appendDescription("headers:" + headers);
                return;
            }
            if (headerReceived) {
                transportError = TriRpcStatus.INTERNAL.withDescription("Received headers twice");
                return;
            }
            Integer httpStatus = headers.status() == null ? null : Integer.parseInt(headers.status()
                    .toString());

            if (httpStatus != null && Integer.parseInt(httpStatus.toString()) > 100 && httpStatus < 200) {
                // ignored
                return;
            }
            headerReceived = true;

            transportError = validateHeaderStatus(headers);

            // todo support full payload compressor
            CharSequence messageEncoding = headers.get(TripleHeaderEnum.GRPC_ENCODING.getHeader());
            CharSequence triExceptionCode = headers.get(TripleHeaderEnum.TRI_EXCEPTION_CODE.getHeader());
            if (triExceptionCode != null) {
                Integer triExceptionCodeNum = Integer.parseInt(triExceptionCode.toString());
                if (!(triExceptionCodeNum.equals(CommonConstants.TRI_EXCEPTION_CODE_NOT_EXISTS))) {
                    isReturnTriException = true;
                }
            }
            if (null != messageEncoding) {
                String compressorStr = messageEncoding.toString();
                if (!Identity.IDENTITY.getMessageEncoding()
                        .equals(compressorStr)) {
                    DeCompressor compressor = DeCompressor.getCompressor(frameworkModel, compressorStr);
                    if (null == compressor) {
                        throw TriRpcStatus.UNIMPLEMENTED.withDescription(String.format(
                                        "Grpc-encoding '%s' is not " + "supported", compressorStr))
                                .asException();
                    } else {
                        decompressor = compressor;
                    }
                }
            }
            TriDecoder.Listener listener = new TriDecoder.Listener() {
                @Override
                public void onRawMessage(byte[] data) {
                    TripleHttp3ClientStream.this.listener.onMessage(data, isReturnTriException);
                }

                public void close() {
                    finishProcess(statusFromTrailers(trailers), trailers, isReturnTriException);
                }
            };
            deframer = new TriDecoder(decompressor, listener);
            TripleHttp3ClientStream.this.listener.onStart();
        }

        // todo 需要重写
        private TriRpcStatus validateHeaderStatus(Http3Headers headers) {
            Integer httpStatus = headers.status() == null ? null : Integer.parseInt(headers.status()
                    .toString());
            if (httpStatus == null) {
                return TriRpcStatus.INTERNAL.withDescription("Missing HTTP status code");
            }
            final CharSequence contentType = headers.get(TripleHeaderEnum.CONTENT_TYPE_KEY.getHeader());
            if (contentType == null || !contentType.toString()
                    .startsWith(TripleHeaderEnum.APPLICATION_GRPC.getHeader())) {
                return TriRpcStatus.fromCode(TriRpcStatus.httpStatusToGrpcCode(httpStatus))
                        .withDescription("invalid content-type: " + contentType);
            }
            return null;
        }

        private TriRpcStatus statusFromTrailers(Http3Headers trailers) {
            final Integer intStatus = trailers.getInt(TripleHeaderEnum.STATUS_KEY.getHeader());
            TriRpcStatus status = intStatus == null ? null : TriRpcStatus.fromCode(intStatus);
            if (status != null) {
                final CharSequence message = trailers.get(TripleHeaderEnum.MESSAGE_KEY.getHeader());
                if (message != null) {
                    final String description = TriRpcStatus.decodeMessage(message.toString());
                    status = status.withDescription(description);
                }
                return status;
            }
            // No status; something is broken. Try to provide a rational error.
            if (headerReceived) {
                return TriRpcStatus.UNKNOWN.withDescription("missing GRPC status in response");
            }
            Integer httpStatus = trailers.status() == null ? null : Integer.parseInt(trailers.status()
                    .toString());
            if (httpStatus != null) {
                status = TriRpcStatus.fromCode(TriRpcStatus.httpStatusToGrpcCode(httpStatus));
            } else {
                status = TriRpcStatus.INTERNAL.withDescription("missing HTTP status code");
            }
            return status.appendDescription("missing GRPC status, inferred error from HTTP status code");
        }

        @Override
        public void onData(ByteBuf data, boolean endStream) {
            try {
                executor.execute(() -> doOnData(data, endStream));
            } catch (Throwable t) {
                // Tasks will be rejected when the thread pool is closed or full,
                // ByteBuf needs to be released to avoid out of heap memory leakage.
                // For example, ThreadLessExecutor will be shutdown when request timeout {@link AsyncRpcResult}
                ReferenceCountUtil.release(data);
                LOGGER.error(PROTOCOL_FAILED_RESPONSE, "", "", "submit onData task failed", t);
            }
        }

        private void doOnData(ByteBuf data, boolean endStream) {
            if (transportError != null) {
                transportError.appendDescription("Data:" + data.toString(StandardCharsets.UTF_8));
                ReferenceCountUtil.release(data);
                if (transportError.description.length() > 512 || endStream) {
                    handleH3TransportError(transportError);
                }
                return;
            }
            if (!headerReceived) {
                handleH3TransportError(TriRpcStatus.INTERNAL.withDescription("headers not received before payload"));
                return;
            }
            deframer.deframe(data);
        }

        // todo 需要重写
        void handleH3TransportError(TriRpcStatus status) {
            writeQueue.enqueue(QuicCancelQueueCommand.createCommand(http3StreamChannelFuture,
                    Http3ErrorCode.H3_REQUEST_CANCELLED));
            TripleHttp3ClientStream.this.rst = true;
            finishProcess(status, null, false);
        }

        // todo 需要重写
        void finishProcess(TriRpcStatus status, Http3Headers trailers, boolean isReturnTriException) {
            final Map<String, String> reserved = filterReservedHeaders(trailers);
            final Map<String, Object> attachments = headersToMap(trailers,
                    () -> reserved.get(TripleHeaderEnum.TRI_HEADER_CONVERT.getHeader()));
            final TriRpcStatus detailStatus;
            final TriRpcStatus statusFromTrailers = getStatusFromTrailers(reserved);
            if (statusFromTrailers != null) {
                detailStatus = statusFromTrailers;
            } else {
                detailStatus = status;
            }
            listener.onComplete(detailStatus, attachments, reserved, isReturnTriException);
        }

        protected Map<String, String> filterReservedHeaders(Http3Headers trailers) {
            if (trailers == null) {
                return Collections.emptyMap();
            }
            Map<String, String> excludeHeaders = new HashMap<>(trailers.size());
            for (Map.Entry<CharSequence, CharSequence> header : trailers) {
                String key = header.getKey()
                        .toString();
                if (TripleHeaderEnum.containsExcludeAttachments(key)) {
                    excludeHeaders.put(key, trailers.getAndRemove(key)
                            .toString());
                }
            }
            return excludeHeaders;
        }

        protected Map<String, Object> headersToMap(Http3Headers trailers, Supplier<Object> convertUpperHeaderSupplier) {
            if (trailers == null) {
                return Collections.emptyMap();
            }
            Map<String, Object> attachments = new HashMap<>(trailers.size());
            for (Map.Entry<CharSequence, CharSequence> header : trailers) {
                String key = header.getKey()
                        .toString();
                if (key.endsWith(TripleConstant.HEADER_BIN_SUFFIX)
                        && key.length() > TripleConstant.HEADER_BIN_SUFFIX.length()) {
                    try {
                        String realKey = key.substring(0, key.length() - TripleConstant.HEADER_BIN_SUFFIX.length());
                        byte[] value = StreamUtils.decodeASCIIByte(header.getValue()
                                .toString());
                        attachments.put(realKey, value);
                    } catch (Exception e) {
                        LOGGER.error(PROTOCOL_FAILED_PARSE, "", "",
                                "Failed to parse response attachment key=" + key, e);
                    }
                } else {
                    attachments.put(key, header.getValue()
                            .toString());
                }
            }

            // try converting upper key
            Object obj = convertUpperHeaderSupplier.get();
            if (obj == null) {
                return attachments;
            }
            if (obj instanceof String) {
                String json = TriRpcStatus.decodeMessage((String) obj);
                Map<String, String> map = JsonUtils.toJavaObject(json, Map.class);
                for (Map.Entry<String, String> entry : map.entrySet()) {
                    Object val = attachments.remove(entry.getKey());
                    if (val != null) {
                        attachments.put(entry.getValue(), val);
                    }
                }
            } else {
                // If convertUpperHeaderSupplier does not return String, just fail...
                // Internal invocation, use INTERNAL_ERROR instead.

                LOGGER.error(INTERNAL_ERROR, "wrong internal invocation", "",
                        "Triple convertNoLowerCaseHeader error," + " obj is not String");
            }
            return attachments;
        }

        private TriRpcStatus getStatusFromTrailers(Map<String, String> metadata) {
            if (null == metadata) {
                return null;
            }
            if (!getGrpcStatusDetailEnabled()) {
                return null;
            }
            // second get status detail
            if (!metadata.containsKey(TripleHeaderEnum.STATUS_DETAIL_KEY.getHeader())) {
                return null;
            }
            final String raw = (metadata.remove(TripleHeaderEnum.STATUS_DETAIL_KEY.getHeader()));
            byte[] statusDetailBin = StreamUtils.decodeASCIIByte(raw);
            ClassLoader tccl = Thread.currentThread()
                    .getContextClassLoader();
            try {
                final Status statusDetail = Status.parseFrom(statusDetailBin);
                List<Any> detailList = statusDetail.getDetailsList();
                Map<Class<?>, Object> classObjectMap = tranFromStatusDetails(detailList);

                // get common exception from DebugInfo
                TriRpcStatus status = TriRpcStatus.fromCode(statusDetail.getCode())
                        .withDescription(TriRpcStatus.decodeMessage(statusDetail.getMessage()));
                DebugInfo debugInfo = (DebugInfo) classObjectMap.get(DebugInfo.class);
                if (debugInfo != null) {
                    String msg = ExceptionUtils.getStackFrameString(debugInfo.getStackEntriesList());
                    status = status.appendDescription(msg);
                }
                return status;
            } catch (IOException ioException) {
                return null;
            } finally {
                ClassLoadUtil.switchContextLoader(tccl);
            }
        }

        private Map<Class<?>, Object> tranFromStatusDetails(List<Any> detailList) {
            Map<Class<?>, Object> map = new HashMap<>(detailList.size());
            try {
                for (Any any : detailList) {
                    if (any.is(ErrorInfo.class)) {
                        ErrorInfo errorInfo = any.unpack(ErrorInfo.class);
                        map.putIfAbsent(ErrorInfo.class, errorInfo);
                    } else if (any.is(DebugInfo.class)) {
                        DebugInfo debugInfo = any.unpack(DebugInfo.class);
                        map.putIfAbsent(DebugInfo.class, debugInfo);
                    }
                    // support others type but now only support this
                }
            } catch (Throwable t) {
                LOGGER.error(PROTOCOL_FAILED_RESPONSE, "", "", "tran from grpc-status-details error", t);
            }
            return map;
        }

        @Override
        public void cancelByRemote(long errorCode) {
            executor.execute(() -> {
                transportError = TriRpcStatus.CANCELLED.withDescription(
                        "Canceled by remote peer, errorCode=" + errorCode);
                finishProcess(transportError, null, false);
            });
        }
    }
}
