/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.remoting.http3.netty4;

import org.apache.dubbo.remoting.http12.AbstractServerHttpChannelObserver;
import org.apache.dubbo.remoting.http12.ErrorCodeHolder;
import org.apache.dubbo.remoting.http12.FlowControlStreamObserver;
import org.apache.dubbo.remoting.http12.HttpChannel;
import org.apache.dubbo.remoting.http12.HttpChannelObserver;
import org.apache.dubbo.remoting.http12.HttpHeaderNames;
import org.apache.dubbo.remoting.http12.HttpHeaders;
import org.apache.dubbo.remoting.http12.HttpMetadata;
import org.apache.dubbo.remoting.http12.h2.H2StreamChannel;
import org.apache.dubbo.remoting.http12.h2.Http2MetadataFrame;
import org.apache.dubbo.remoting.http12.message.StreamingDecoder;
import org.apache.dubbo.rpc.CancellationContext;

public class Http3ServerChannelObserver extends AbstractServerHttpChannelObserver
        implements HttpChannelObserver<Object>,
        FlowControlStreamObserver<Object>,
        Http3CancelableStreamObserver<Object> {

    private CancellationContext cancellationContext;

    private StreamingDecoder streamingDecoder;

    private boolean autoRequestN = true;

    public Http3ServerChannelObserver(Http3StreamChannel http3StreamChannel) {
        super((HttpChannel) http3StreamChannel);
    }

    public void setStreamingDecoder(StreamingDecoder streamingDecoder) {
        this.streamingDecoder = streamingDecoder;
    }

    @Override
    protected HttpMetadata encodeHttpMetadata() {
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.set(HttpHeaderNames.TE.getName(), "trailers");
        return new Http2MetadataFrame(httpHeaders);
    }

    @Override
    protected HttpMetadata encodeTrailers(Throwable throwable) {
        return new Http2MetadataFrame(new HttpHeaders(), true);
    }

    @Override
    public H2StreamChannel getHttpChannel() {
        return (H2StreamChannel) super.getHttpChannel();
    }

    @Override
    public void setCancellationContext(CancellationContext cancellationContext) {
        this.cancellationContext = cancellationContext;
    }

    @Override
    public CancellationContext getCancellationContext() {
        return cancellationContext;
    }

    @Override
    public void cancel(Throwable throwable) {
        long errorCode = 0;
        if (throwable instanceof ErrorCodeHolder) {
            errorCode = ((ErrorCodeHolder) throwable).getErrorCode();
        }
        getHttpChannel().writeResetFrame(errorCode);
        this.cancellationContext.cancel(throwable);
    }

    @Override
    public void request(int count) {
        this.streamingDecoder.request(count);
    }

    @Override
    public void disableAutoFlowControl() {
        this.autoRequestN = false;
    }

    @Override
    public boolean isAutoRequestN() {
        return autoRequestN;
    }
}
