package org.apache.dubbo.remoting.http3.netty4;

import io.netty.incubator.codec.http3.Http3DataFrame;
import io.netty.incubator.codec.http3.Http3HeadersFrame;

public interface CancelableTransportListener<HEADER extends Http3HeadersFrame, MESSAGE extends Http3DataFrame>
        extends HttpTransportListener<HEADER, MESSAGE> {

    void cancelByRemote(long errorCode);
}
