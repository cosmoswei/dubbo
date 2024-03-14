package org.apache.dubbo.rpc.protocol.tri.stream.h3;

import org.apache.dubbo.rpc.TriRpcStatus;
import org.apache.dubbo.rpc.model.FrameworkModel;
import org.apache.dubbo.rpc.protocol.tri.frame.Deframer;
import org.apache.dubbo.rpc.protocol.tri.stream.AbstractStream;
import org.apache.dubbo.rpc.protocol.tri.stream.ClientStream;
import org.apache.dubbo.rpc.protocol.tri.transport.TripleWriteQueue;
import org.apache.dubbo.rpc.protocol.tri.transport.h3.Http3TransportListener;

import java.net.SocketAddress;
import java.util.concurrent.Executor;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.incubator.codec.http3.DefaultHttp3DataFrame;
import io.netty.incubator.codec.http3.DefaultHttp3HeadersFrame;
import io.netty.incubator.codec.http3.Http3;
import io.netty.incubator.codec.http3.Http3DataFrame;
import io.netty.incubator.codec.http3.Http3Headers;
import io.netty.incubator.codec.http3.Http3HeadersFrame;
import io.netty.incubator.codec.http3.Http3RequestStreamInboundHandler;
import io.netty.incubator.codec.quic.QuicChannel;
import io.netty.incubator.codec.quic.QuicStreamChannel;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.Future;

public class TripleHttp3ClientStream extends AbstractStream implements ClientStream {

    public final ClientStream.Listener listener;
    private final TripleWriteQueue writeQueue;
    private Deframer deframer;
    private final TripleHttp3StreamChannelFuture tripleHttp3StreamChannelFuture;
    private boolean halfClosed;
    private boolean rst;

    private boolean isReturnTriException = false;

    private final QuicChannel parentChannel;
    private final QuicStreamChannel streamChannel;

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
        this.streamChannel = initHttp3StreamChannel();
        this.tripleHttp3StreamChannelFuture = initTripleHttp3StreamChannelFuture();
    }

    private TripleHttp3StreamChannelFuture initTripleHttp3StreamChannelFuture() {
        return null;
    }

    private QuicStreamChannel initHttp3StreamChannel() {
        try {
            return Http3.newRequestStream(parentChannel, new Http3RequestStreamInboundHandler() {
                        @Override
                        protected void channelRead(ChannelHandlerContext ctx, Http3HeadersFrame frame) {
                            ReferenceCountUtil.release(frame);
                        }

                        @Override
                        protected void channelRead(ChannelHandlerContext ctx, Http3DataFrame frame) {
                            System.err.print(frame.content()
                                    .toString(CharsetUtil.US_ASCII));
                            ReferenceCountUtil.release(frame);
                        }

                        @Override
                        protected void channelInputClosed(ChannelHandlerContext ctx) {
                            ctx.close();
                        }
                    })
                    .sync()
                    .getNow();
        } catch (InterruptedException e) {
            throw new RuntimeException("Failed to create HTTP/3 stream", e);
        }
    }

    // 发送请求，异步返回
    public Future<?> sendMessage(byte[] message, int compressFlag, boolean eos) {
        ByteBuf buf = streamChannel.alloc()
                .buffer();
        buf.writeByte(compressFlag);
        buf.writeInt(message.length);
        buf.writeBytes(message);
        ChannelPromise channelPromise = streamChannel.newPromise();
        streamChannel.write(new DefaultHttp3DataFrame(buf), channelPromise);

        return channelPromise;
    }

    public Future<?> halfClose() {
        return null;
    }

    public Future<?> sendHeader(Http3Headers headers) {
        DefaultHttp3HeadersFrame frame = new DefaultHttp3HeadersFrame();
        frame.headers()
                .setAll(headers);

        return streamChannel.writeAndFlush(frame);
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

        @Override
        public void onHeader(Http3Headers headers, boolean endStream) {

        }

        @Override
        public void onData(ByteBuf data, boolean endStream) {

        }

        @Override
        public void cancelByRemote(long errorCode) {

        }
    }
}
