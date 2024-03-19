package org.apache.dubbo.remoting.http3.netty4;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.remoting.http12.HttpHeaderNames;
import org.apache.dubbo.remoting.http12.HttpHeaders;
import org.apache.dubbo.remoting.http12.HttpMetadata;
import org.apache.dubbo.remoting.http12.command.HttpWriteQueue;
import org.apache.dubbo.remoting.http12.exception.UnsupportedMediaTypeException;
import org.apache.dubbo.remoting.http12.netty4.HttpWriteQueueHandler;
import org.apache.dubbo.remoting.http3.netty4.command.Http3WriteQueueChannel;
import org.apache.dubbo.rpc.model.FrameworkModel;

import java.util.Set;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.incubator.codec.quic.QuicStreamChannel;

public class NettyHttp3ProtocolSelectorHandler extends SimpleChannelInboundHandler<HttpMetadata> {

    private final URL url;

    private final FrameworkModel frameworkModel;

    private final Http3ServerTransportListenerFactory defaultHttp3ServerTransportListenerFactory;

    public NettyHttp3ProtocolSelectorHandler(
            URL url,
            FrameworkModel frameworkModel,
            Http3ServerTransportListenerFactory defaultHttp3ServerTransportListenerFactory) {
        this.url = url;
        this.frameworkModel = frameworkModel;
        this.defaultHttp3ServerTransportListenerFactory = defaultHttp3ServerTransportListenerFactory;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, HttpMetadata metadata) {
        HttpHeaders headers = metadata.headers();
        String contentType = headers.getFirst(HttpHeaderNames.CONTENT_TYPE.getName());
        Http3ServerTransportListenerFactory factory = determineHttp3ServerTransportListenerFactory(contentType);
        if (factory == null) {
            throw new UnsupportedMediaTypeException(contentType);
        }
        QuicStreamChannel quicStreamChannel = (QuicStreamChannel) ctx.channel();

        Http3StreamChannel http3StreamChannel = new NettyHttp3StreamChannel(quicStreamChannel);
        HttpWriteQueueHandler writeQueueHandler = quicStreamChannel.pipeline()
                .get(HttpWriteQueueHandler.class);

        if (writeQueueHandler != null) {
            HttpWriteQueue writeQueue = writeQueueHandler.getWriteQueue();
            http3StreamChannel = new Http3WriteQueueChannel(http3StreamChannel, writeQueue);
        }

        ChannelPipeline pipeline = ctx.pipeline();
        pipeline.addLast(new NettyHttp3FrameHandler(http3StreamChannel, factory.newInstance(http3StreamChannel, url,
                frameworkModel)));
        pipeline.remove(this);
        ctx.fireChannelRead(metadata);
    }

    private Http3ServerTransportListenerFactory determineHttp3ServerTransportListenerFactory(String contentType) {
        Set<Http3ServerTransportListenerFactory> http3ServerTransportListenerFactories =
                frameworkModel.getExtensionLoader(Http3ServerTransportListenerFactory.class)
                .getSupportedExtensionInstances();
        for (Http3ServerTransportListenerFactory factory : http3ServerTransportListenerFactories) {
            if (factory.supportContentType(contentType)) {
                return factory;
            }
        }
        return defaultHttp3ServerTransportListenerFactory;
    }
}
