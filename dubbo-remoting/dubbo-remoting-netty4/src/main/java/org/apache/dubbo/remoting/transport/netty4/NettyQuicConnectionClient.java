package org.apache.dubbo.remoting.transport.netty4;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.ChannelHandler;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.api.WireProtocol;
import org.apache.dubbo.remoting.api.connection.AbstractConnectionClient;

import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.socket.DatagramChannel;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.netty.util.concurrent.Promise;

public class NettyQuicConnectionClient extends AbstractConnectionClient {

    private static final ErrorTypeAwareLogger LOGGER =
            LoggerFactory.getErrorTypeAwareLogger(NettyConnectionClient.class);

    private AtomicReference<Promise<Object>> connectingPromise;

    private Promise<Void> closePromise;

    private AtomicReference<io.netty.channel.Channel> channel;

    private ConnectionListener connectionListener;

    private Bootstrap bootstrap;

    public static final AttributeKey<AbstractConnectionClient> CONNECTION = AttributeKey.valueOf("connection");

    public NettyQuicConnectionClient(URL url, ChannelHandler handler) throws RemotingException {
        super(url, handler);
    }

    @Override
    protected void doOpen() throws Throwable {
        initConnectionClient();
        initBootstrap();
    }

    @Override
    protected void doClose() throws Throwable {

    }

    @Override
    protected void doConnect() throws Throwable {
        if (isClosed()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(String.format("%s aborted to reconnect cause connection closed. ",
                        NettyQuicConnectionClient.this));
            }
        }
        init.compareAndSet(false, true);

        io.netty.channel.Channel channel = bootstrap.bind(0)
                .sync()
                .channel();

        io.netty.channel.Channel quicChannel = newQuicChannel(channel);
        onConnected(quicChannel);
    }

    @Override
    protected void doDisConnect() throws Throwable {

    }

    @Override
    public InetSocketAddress getRemoteAddress() {
        return getUrl().toInetSocketAddress();
    }

    @Override
    protected Channel getChannel() {
        io.netty.channel.Channel c = channel.get();
        if (c == null) {
            return null;
        }
        return NettyChannel.getOrAddChannel(c, getUrl(), this);
    }

    @Override
    protected void initConnectionClient() {
        this.protocol = getUrl().getOrDefaultFrameworkModel()
                .getExtensionLoader(WireProtocol.class)
                .getExtension(getUrl().getProtocol());
        this.remote = getConnectAddress();
        this.connectingPromise = new AtomicReference<>();
        this.connectionListener = new ConnectionListener();
        this.channel = new AtomicReference<>();
        this.closePromise = new DefaultPromise<>(GlobalEventExecutor.INSTANCE);
        this.init = new AtomicBoolean(false);
        this.increase();

    }

    @Override
    public boolean isAvailable() {
        return false;
    }

    @Override
    public void createConnectingPromise() {

    }

    @Override
    public void addCloseListener(Runnable func) {

    }

    @Override
    public void onConnected(Object channel) {
        if (!(channel instanceof io.netty.channel.Channel)) {
            return;
        }
        io.netty.channel.Channel nettyChannel = ((io.netty.channel.Channel) channel);
        if (isClosed()) {
            nettyChannel.close();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(String.format("%s is closed, ignoring connected event", this));
            }
            return;
        }
        this.channel.set(nettyChannel);
        // This indicates that the connection is available.
        if (this.connectingPromise.get() != null) {
            this.connectingPromise.get()
                    .trySuccess(CONNECTED_OBJECT);
        }
        nettyChannel.attr(CONNECTION)
                .set(this);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(String.format("%s connected ", this));
        }
    }

    @Override
    public void onGoaway(Object channel) {

    }

    @Override
    public void destroy() {

    }

    @Override
    public Object getChannel(Boolean generalizable) {
        return Boolean.TRUE.equals(generalizable) ? channel.get() : getChannel();
    }

    private void initBootstrap() {
        final Bootstrap nettyBootstrap = new Bootstrap();

        nettyBootstrap.group(NettyEventLoopFactory.NIO_EVENT_LOOP_GROUP.get())
                .channel(NettyEventLoopFactory.datagramChannelClass())
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, getConnectTimeout());

        nettyBootstrap.handler(new ChannelInitializer<DatagramChannel>() {
            @Override
            protected void initChannel(DatagramChannel datagramChannel) {
                org.apache.dubbo.remoting.transport.netty4.NettyChannel nettyChannel =
                        NettyChannel.getOrAddChannel(datagramChannel, getUrl(), getChannelHandler());

                NettySslContextOperator nettySslContextOperator = new NettySslContextOperator();
                NettyConfigOperator operator = new NettyConfigOperator(nettyChannel, getChannelHandler());
                protocol.configClientPipeline(getUrl(), operator, nettySslContextOperator);
            }
        });

        this.bootstrap = nettyBootstrap;
    }

    private io.netty.channel.Channel newQuicChannel(io.netty.channel.Channel parent) {
        try {
            Object quicChannelBootstrap = newQuicChannelBootstrap(parent);
            Class<?> quicChannelBootstrapClass = Class.forName("io.netty.incubator.codec.quic.QuicChannelBootstrap");
            Method handlerMethod = quicChannelBootstrapClass.getDeclaredMethod("handler",
                    io.netty.channel.ChannelHandler.class);
            handlerMethod.invoke(quicChannelBootstrap, newHttp3ClientConnectionHandler());
            Method remoteAddressMethod = quicChannelBootstrapClass.getDeclaredMethod("remoteAddress",
                    SocketAddress.class);

            remoteAddressMethod.invoke(quicChannelBootstrap, getRemoteAddress());

            Method connectMethod = quicChannelBootstrapClass.getDeclaredMethod("connect");

            Future<?> future = (Future<?>) connectMethod.invoke(quicChannelBootstrap);
            return (io.netty.channel.Channel) future.get();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to create QuicChannel", e);
        }
    }

    private Object newQuicChannelBootstrap(io.netty.channel.Channel parent) {
        try {
            Class<?> quicChannelClass = Class.forName("io.netty.incubator.codec.quic.QuicChannel");
            Method newBootstrapMethod = quicChannelClass.getDeclaredMethod("newBootstrap",
                    io.netty.channel.Channel.class);
            return newBootstrapMethod.invoke(null, parent);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to create QuicChannelBootstrap", e);
        }
    }

    private io.netty.channel.ChannelHandler newHttp3ClientConnectionHandler() {
        try {
            String className = "io.netty.incubator.codec.http3.Http3ClientConnectionHandler";
            Class<?> http3ClientConnectionHandlerClass = Class.forName(className);
            return (io.netty.channel.ChannelHandler) http3ClientConnectionHandlerClass.newInstance();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to create Http3ClientConnectionHandler", e);
        }
    }

    static class ConnectionListener implements ChannelFutureListener {

        @Override
        public void operationComplete(ChannelFuture future) {
            // TODO
        }
    }
}
