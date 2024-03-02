package org.apache.dubbo.rpc.protocol.tri.call;

import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.stream.StreamObserver;
import org.apache.dubbo.remoting.api.connection.AbstractConnectionClient;
import org.apache.dubbo.rpc.TriRpcStatus;
import org.apache.dubbo.rpc.protocol.tri.RequestMetadata;
import org.apache.dubbo.rpc.protocol.tri.compressor.Identity;
import org.apache.dubbo.rpc.protocol.tri.observer.ClientCallToObserverAdapter;
import org.apache.dubbo.rpc.protocol.tri.stream.TripleOverHttp3ClientStream;

import io.netty.incubator.codec.quic.QuicChannel;

import static org.apache.dubbo.common.constants.LoggerCodeConstants.PROTOCOL_FAILED_SERIALIZE_TRIPLE;

public class TripleOverHttp3ClientCall implements ClientCall {
    private static final ErrorTypeAwareLogger LOGGER = LoggerFactory.getErrorTypeAwareLogger(TripleClientCall.class);
    private final AbstractConnectionClient connectionClient;
    private RequestMetadata requestMetadata;
    private ClientCall.Listener listener;
    private TripleOverHttp3ClientStream stream;

    private boolean headerSent = false;

    public TripleOverHttp3ClientCall(AbstractConnectionClient connectionClient) {
        this.connectionClient = connectionClient;
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
            stream.sendMessage(compress, compressed, false).addListener(f -> {
                if (!f.isSuccess()) {
                    cancelByLocal(f.cause());
                }
            });
        } catch (Throwable t) {
            LOGGER.error(
                    PROTOCOL_FAILED_SERIALIZE_TRIPLE,
                    "",
                    "",
                    String.format(
                            "Serialize triple request failed, service=%s method=%s",
                            requestMetadata.service, requestMetadata.method.getMethodName()),
                    t);
            cancelByLocal(t);
            listener.onClose(
                    TriRpcStatus.INTERNAL
                            .withDescription("Serialize request failed")
                            .withCause(t),
                    null,
                    false);
        }
    }

    @Override
    public StreamObserver<Object> start(RequestMetadata metadata, ClientCall.Listener responseListener) {
        this.requestMetadata = metadata;
        this.listener = responseListener;
        this.stream = new TripleOverHttp3ClientStream((QuicChannel) connectionClient.getChannel(true));
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
}
