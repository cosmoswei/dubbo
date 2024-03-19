package org.apache.dubbo.remoting.http3.netty4;

import org.apache.dubbo.remoting.http12.HttpHeaders;
import org.apache.dubbo.remoting.http12.HttpInputMessage;
import org.apache.dubbo.remoting.http12.h2.Http2Header;
import org.apache.dubbo.remoting.http12.h2.Http2InputMessage;
import org.apache.dubbo.remoting.http12.h2.Http2InputMessageFrame;
import org.apache.dubbo.remoting.http12.h2.Http2MetadataFrame;

import java.util.Map;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.incubator.codec.http3.Http3DataFrame;
import io.netty.incubator.codec.http3.Http3Headers;
import io.netty.incubator.codec.http3.Http3HeadersFrame;

public class NettyHttp3FrameCodec extends ChannelDuplexHandler {

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof Http3HeadersFrame) {
            Http3Header http3Header = onHttp3HeadersFrame((Http3HeadersFrame) msg);
            super.channelRead(ctx, http3Header);
        } else if (msg instanceof Http3DataFrame) {
            Http3InputMessage http2InputMessage = onHttp3DataFrame((Http3DataFrame) msg);
            super.channelRead(ctx, http2InputMessage);
        } else {
            super.channelRead(ctx, msg);
        }
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof Http3HeadersFrame) {
            Http3HeadersFrame http3HeadersFrame = (Http3HeadersFrame) msg;
            Http3Header http3Header = onHttp3HeadersFrame(http3HeadersFrame);
            super.write(ctx, http3Header, promise);
        } else if (msg instanceof Http3DataFrame) {
            Http3InputMessage http3InputMessage = onHttp3DataFrame((Http3DataFrame) msg);
            super.write(ctx, http3InputMessage, promise);
        } else {
            super.write(ctx, msg, promise);
        }
    }

    private Http3Header onHttp3HeadersFrame(Http3HeadersFrame headersFrame) {
        Http3Headers headers = headersFrame.headers();
        HttpHeaders head = new HttpHeaders();
        for (Map.Entry<CharSequence, CharSequence> header : headers) {
            head.set(header.getKey()
                    .toString(), header.getValue()
                    .toString());
        }
        return new Http3MetadataFrame(head);
    }

    private Http3InputMessage onHttp3DataFrame(Http3DataFrame dataFrame) {
        ByteBuf content = dataFrame.content();
        return new Http3InputMessageFrame(new ByteBufInputStream(content, true), true);
    }
}
