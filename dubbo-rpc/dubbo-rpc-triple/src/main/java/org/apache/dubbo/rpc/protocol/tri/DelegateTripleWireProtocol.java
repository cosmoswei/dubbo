package org.apache.dubbo.rpc.protocol.tri;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.remoting.api.AbstractWireProtocol;
import org.apache.dubbo.remoting.api.ProtocolDetector.Result;
import org.apache.dubbo.remoting.api.pu.ChannelOperator;
import org.apache.dubbo.remoting.api.ssl.ContextOperator;
import org.apache.dubbo.rpc.model.FrameworkModel;
import org.apache.dubbo.rpc.model.ScopeModelAware;
import org.apache.dubbo.rpc.protocol.tri.h12.TripleProtocolDetector;

@Activate
public class DelegateTripleWireProtocol extends AbstractWireProtocol implements ScopeModelAware {

    private final TripleHttp2Protocol tripleHttp2Protocol;
    private final TripleHttp3Protocol tripleHttp3Protocol;

    public DelegateTripleWireProtocol() {
        super(new TripleProtocolDetector());
        tripleHttp2Protocol = new TripleHttp2Protocol();
        tripleHttp3Protocol = new TripleHttp3Protocol();
    }

    @Override
    public void configClientPipeline(URL url, ChannelOperator operator, ContextOperator contextOperator) {
        String isQuicEnabled = url.getParameter(CommonConstants.QUIC_ENABLED_KEY);

        if (Boolean.parseBoolean(isQuicEnabled)) {
            tripleHttp3Protocol.configClientPipeline(url, operator, contextOperator);
        } else {
            tripleHttp2Protocol.configClientPipeline(url, operator, contextOperator);
        }
    }

    @Override
    public void configServerProtocolHandler(URL url, ChannelOperator operator) {
        Result result = operator.detectResult();
        // the not detected protocol will be handled by http3
        if (result == null) {
            tripleHttp3Protocol.configServerProtocolHandler(url, operator);
        } else {
            tripleHttp2Protocol.configServerProtocolHandler(url, operator);
        }
    }

    @Override
    public void setFrameworkModel(FrameworkModel frameworkModel) {
        tripleHttp2Protocol.setFrameworkModel(frameworkModel);
        tripleHttp3Protocol.setFrameworkModel(frameworkModel);
    }
}
