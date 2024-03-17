package org.apache.dubbo.rpc.protocol.tri.call;

import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.stream.StreamObserver;
import org.apache.dubbo.remoting.api.connection.AbstractConnectionClient;
import org.apache.dubbo.rpc.TriRpcStatus;
import org.apache.dubbo.rpc.model.FrameworkModel;
import org.apache.dubbo.rpc.protocol.tri.RequestMetadata;
import org.apache.dubbo.rpc.protocol.tri.compressor.Identity;
import org.apache.dubbo.rpc.protocol.tri.observer.ClientCallToObserverAdapter;
import org.apache.dubbo.rpc.protocol.tri.stream.ClientStream;
import org.apache.dubbo.rpc.protocol.tri.stream.h3.TripleHttp3ClientStream;
import org.apache.dubbo.rpc.protocol.tri.transport.TripleWriteQueue;

import java.util.concurrent.Executor;

import io.netty.incubator.codec.quic.QuicChannel;

import static org.apache.dubbo.common.constants.LoggerCodeConstants.PROTOCOL_FAILED_RESPONSE;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.PROTOCOL_FAILED_SERIALIZE_TRIPLE;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.PROTOCOL_STREAM_LISTENER;

public class TripleHttp3ClientCall implements ClientCall, ClientStream.Listener {
    private static final ErrorTypeAwareLogger LOGGER =
            LoggerFactory.getErrorTypeAwareLogger(TripleHttp3ClientCall.class);
    // 连接客户端
    private final AbstractConnectionClient connectionClient;
    // 业务线程池
    private final Executor executor;
    private final FrameworkModel frameworkModel;
    // 写入队列
    private final TripleWriteQueue writeQueue;
    // 请求元数据
    private RequestMetadata requestMetadata;
    // HTTP3 的流
    private TripleHttp3ClientStream stream;
    // 连接状态监听器
    private ClientCall.Listener listener;
    private boolean canceled;
    private boolean autoRequest = true;
    private boolean done;
    private boolean headerSent;

    public TripleHttp3ClientCall(
            AbstractConnectionClient connectionClient,
            Executor executor,
            FrameworkModel frameworkModel,
            TripleWriteQueue writeQueue) {
        this.connectionClient = connectionClient;
        this.executor = executor;
        this.frameworkModel = frameworkModel;
        this.writeQueue = writeQueue;
    }

    @Override
    public void cancelByLocal(Throwable t) {

    }

    @Override
    public void request(int messageNumber) {

    }

    @Override
    public void sendMessage(Object message) {
        if (!headerSent) {
            headerSent = true;
            stream.sendHeader(requestMetadata.toHttp3Headers());
        }

        final byte[] data;
        try {
            data = requestMetadata.packableMethod.packRequest(message);
            int compressed = Identity.MESSAGE_ENCODING.equals(requestMetadata.compressor.getMessageEncoding()) ? 0 : 1;
            final byte[] compress = requestMetadata.compressor.compress(data);
            stream.sendMessage(compress, compressed, false)
                    .addListener(f -> {
                        if (!f.isSuccess()) {
                            cancelByLocal(f.cause());
                        }
                    });
        } catch (Throwable t) {
            LOGGER.error(PROTOCOL_FAILED_SERIALIZE_TRIPLE, "", "", String.format("Serialize triple request failed, "
                    + "service=%s method=%s", requestMetadata.service, requestMetadata.method.getMethodName()), t);
            cancelByLocal(t);
            listener.onClose(TriRpcStatus.INTERNAL.withDescription("Serialize request failed")
                    .withCause(t), null, false);
        }
    }

    @Override
    public StreamObserver<Object> start(RequestMetadata metadata, ClientCall.Listener responseListener) {
        this.requestMetadata = metadata;
        this.listener = responseListener;
        this.stream = new TripleHttp3ClientStream(frameworkModel, executor, this,
                (QuicChannel) connectionClient.getChannel(true), writeQueue);
        return new ClientCallToObserverAdapter<>(this);
    }

    @Override
    public boolean isAutoRequest() {
        return false;
    }

    @Override
    public void setAutoRequest(boolean autoRequest) {

    }

    @Override
    public void halfClose() {

    }

    @Override
    public void setCompression(String compression) {
    }

    @Override
    public void onStart() {
        listener.onStart(TripleHttp3ClientCall.this);
    }

    @Override
    // 获取返回结果，写入异步结果，返回；
    // 需要在org.apache.dubbo.rpc.protocol.tri.stream.h3.TripleHttp3ClientStream.Http3ClientTransportListener
    // 调用,异步将结果写入Future（“ClientCall.Listener callListener = new UnaryClientCallListener(future);”）
    // 所以在请求的头里面指定
    public void onMessage(byte[] message, boolean isReturnTriException) {
        if (done) {
            LOGGER.warn(PROTOCOL_STREAM_LISTENER, "", "",
                    "Received message from closed stream,connection=" + connectionClient + " service="
                            + requestMetadata.service + " method=" + requestMetadata.method.getMethodName());
            return;
        }
        try {
            final Object unpacked = requestMetadata.packableMethod.parseResponse(message, isReturnTriException);
            listener.onMessage(unpacked, message.length);
        } catch (Throwable t) {
            TriRpcStatus status = TriRpcStatus.INTERNAL.withDescription("Deserialize response failed")
                    .withCause(t);
            cancelByLocal(status.asException());
            listener.onClose(status, null, false);
            LOGGER.error(PROTOCOL_FAILED_RESPONSE, "", "", String.format("Failed to deserialize triple response, "
                    + "service=%s, method=%s,connection=%s", connectionClient, requestMetadata.service,
                    requestMetadata.method.getMethodName()), t);
        }
    }

    @Override
    public void onCancelByRemote(TriRpcStatus status) {

    }
}
