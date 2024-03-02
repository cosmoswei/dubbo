package org.apache.dubbo.rpc.protocol.tri.stream;

import org.apache.dubbo.rpc.TriRpcStatus;

import java.net.SocketAddress;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
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

public class TripleOverHttp3ClientStream {
    private final QuicChannel parentChannel;
    private final QuicStreamChannel streamChannel;

    public TripleOverHttp3ClientStream(QuicChannel parentChannel) {
        this.parentChannel = parentChannel;
        this.streamChannel = initHttp3StreamChannel();
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

    public Future<?> cancelByLocal(TriRpcStatus status) {
        return null;
    }

    public SocketAddress remoteAddress() {
        return null;
    }

    public void request(int n) {

    }
}
