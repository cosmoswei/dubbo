package org.apache.dubbo.remoting.transport.netty4;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.config.ConfigurationUtils;
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.ChannelHandler;
import org.apache.dubbo.remoting.Constants;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.api.WireProtocol;
import org.apache.dubbo.remoting.api.pu.AbstractPortUnificationServer;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.util.concurrent.Future;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.dubbo.common.constants.CommonConstants.ANYHOST_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.ANYHOST_VALUE;
import static org.apache.dubbo.common.constants.LoggerCodeConstants.TRANSPORT_FAILED_CLOSE;

public class NettyQuicPortUnificationServer extends AbstractPortUnificationServer {

    private static final ErrorTypeAwareLogger logger =
            LoggerFactory.getErrorTypeAwareLogger(NettyPortUnificationServer.class);

    private final int serverShutdownTimeoutMills;

    /**
     * netty datagram bootstrap.
     */
    private Bootstrap bootstrap;

    /**
     * the boss channel that receive connections and dispatch these to worker channel.
     */
    private io.netty.channel.Channel channel;

    private EventLoopGroup eventLoopGroup;
    private final Map<String, Channel> dubboChannels = new ConcurrentHashMap<>();

    private WireProtocol protocol;

    public NettyQuicPortUnificationServer(URL url, ChannelHandler handler) throws RemotingException {
        super(url, handler);

        serverShutdownTimeoutMills = ConfigurationUtils.getServerShutdownTimeout(getUrl().getOrDefaultModuleModel());
    }

    @Override
    public boolean isBound() {
        return false;
    }

    @Override
    public Collection<Channel> getChannels() {
        return null;
    }

    @Override
    public Channel getChannel(InetSocketAddress remoteAddress) {
        return null;
    }

    @Override
    protected void doOpen() throws Throwable {
        this.protocol = getUrl().getOrDefaultFrameworkModel()
                .getExtensionLoader(WireProtocol.class)
                .getExtension(getUrl().getProtocol());

        bootstrap = new Bootstrap();

        eventLoopGroup = new NioEventLoopGroup(1);

        bootstrap.group(eventLoopGroup)
                .channel(NioDatagramChannel.class);

        bootstrap.handler(new ChannelInitializer<DatagramChannel>() {
            @Override
            protected void initChannel(DatagramChannel datagramChannel) {
                ChannelHandler channelHandler = getChannelHandler();
                NettyChannel nettyChannel = NettyChannel.getOrAddChannel(datagramChannel, getUrl(), channelHandler);

                NettyConfigOperator operator = new NettyConfigOperator(nettyChannel, channelHandler);
                protocol.configServerProtocolHandler(getUrl(), operator);
            }
        });

        String bindIp = getUrl().getParameter(Constants.BIND_IP_KEY, getUrl().getHost());
        int bindPort = getUrl().getParameter(Constants.BIND_PORT_KEY, getUrl().getPort());
        if (getUrl().getParameter(ANYHOST_KEY, false) || NetUtils.isInvalidLocalHost(bindIp)) {
            bindIp = ANYHOST_VALUE;
        }

        InetSocketAddress bindAddress = new InetSocketAddress(bindIp, bindPort);

        try {
            channel = bootstrap.bind(bindAddress)
                    .sync()
                    .channel();
        } catch (Throwable t) {
            closeBootstrap();
            throw t;
        }
    }

    private void closeBootstrap() {
        try {
            if (bootstrap != null) {
                long timeout = ConfigurationUtils.reCalShutdownTime(serverShutdownTimeoutMills);
                long quietPeriod = Math.min(2000L, timeout);
                Future<?> bossGroupShutdownFuture = eventLoopGroup.shutdownGracefully(quietPeriod, timeout,
                        MILLISECONDS);
                bossGroupShutdownFuture.awaitUninterruptibly(timeout, MILLISECONDS);
            }
        } catch (Throwable e) {
            logger.warn(TRANSPORT_FAILED_CLOSE, "", "", e.getMessage(), e);
        }
    }

    @Override
    protected void doClose() throws Throwable {

    }

    @Override
    protected int getChannelsSize() {
        return 0;
    }
}
