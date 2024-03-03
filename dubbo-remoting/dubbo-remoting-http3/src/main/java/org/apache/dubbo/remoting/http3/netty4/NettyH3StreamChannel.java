package org.apache.dubbo.remoting.http3.netty4;

import org.apache.dubbo.remoting.http12.HttpMetadata;
import org.apache.dubbo.remoting.http12.HttpOutputMessage;
import org.apache.dubbo.remoting.http12.h2.H2StreamChannel;
import org.apache.dubbo.remoting.http12.h2.Http2OutputMessage;
import org.apache.dubbo.remoting.http12.h2.Http2OutputMessageFrame;
import org.apache.dubbo.remoting.http12.netty4.NettyHttpChannelFutureListener;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.CompletableFuture;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.handler.codec.http2.DefaultHttp2ResetFrame;
import io.netty.incubator.codec.http3.DefaultHttp3DataFrame;
import io.netty.incubator.codec.http3.DefaultHttp3HeadersFrame;
import io.netty.incubator.codec.quic.QuicStreamChannel;

public class NettyH3StreamChannel implements H2StreamChannel {

    private final QuicStreamChannel http2StreamChannel;

    public NettyH3StreamChannel(QuicStreamChannel http2StreamChannel) {
        this.http2StreamChannel = http2StreamChannel;
    }

    @Override
    public CompletableFuture<Void> writeHeader(HttpMetadata httpMetadata) {
        // WriteQueue.enqueue header frame
        NettyHttpChannelFutureListener nettyHttpChannelFutureListener = new NettyHttpChannelFutureListener();
        DefaultHttp3HeadersFrame frame = new DefaultHttp3HeadersFrame();
        httpMetadata.headers()
                .forEach((key, value) -> frame.headers()
                        .add(key, value));

        http2StreamChannel.write(frame)
                .addListener(nettyHttpChannelFutureListener);
        return nettyHttpChannelFutureListener;
    }

    @Override
    public CompletableFuture<Void> writeMessage(HttpOutputMessage httpOutputMessage) {
        NettyHttpChannelFutureListener nettyHttpChannelFutureListener = new NettyHttpChannelFutureListener();
        ByteBufOutputStream body = (ByteBufOutputStream) httpOutputMessage.getBody();
        DefaultHttp3DataFrame frame = new DefaultHttp3DataFrame(body.buffer());
        http2StreamChannel.write(frame)
                .addListener(nettyHttpChannelFutureListener);
        return nettyHttpChannelFutureListener;
    }

    @Override
    public Http2OutputMessage newOutputMessage(boolean endStream) {
        ByteBuf buffer = http2StreamChannel.alloc()
                .buffer();
        ByteBufOutputStream outputStream = new ByteBufOutputStream(buffer);
        return new Http2OutputMessageFrame(outputStream, endStream);
    }

    @Override
    public SocketAddress remoteAddress() {
        this.http2StreamChannel.remoteAddress();
        // TODO return the actual remote address
        return new InetSocketAddress(1001);
    }

    @Override
    public SocketAddress localAddress() {
        return this.http2StreamChannel.localAddress();
    }

    @Override
    public void flush() {
        this.http2StreamChannel.flush();
    }

    @Override
    public CompletableFuture<Void> writeResetFrame(long errorCode) {
        DefaultHttp2ResetFrame resetFrame = new DefaultHttp2ResetFrame(errorCode);
        NettyHttpChannelFutureListener nettyHttpChannelFutureListener = new NettyHttpChannelFutureListener();
        http2StreamChannel.write(resetFrame)
                .addListener(nettyHttpChannelFutureListener);
        return nettyHttpChannelFutureListener;
    }
}
