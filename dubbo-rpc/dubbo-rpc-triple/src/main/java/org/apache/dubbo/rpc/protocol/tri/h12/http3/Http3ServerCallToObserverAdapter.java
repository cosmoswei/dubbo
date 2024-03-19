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
package org.apache.dubbo.rpc.protocol.tri.h12.http3;

import org.apache.dubbo.remoting.http3.netty4.Http3StreamChannel;
import org.apache.dubbo.rpc.model.FrameworkModel;
import org.apache.dubbo.rpc.protocol.tri.TripleHeaderEnum;
import org.apache.dubbo.rpc.protocol.tri.h12.ServerCallToObserverAdapter;

public class Http3ServerCallToObserverAdapter extends Http3ServerStreamObserver
        implements ServerCallToObserverAdapter<Object> {

    private int exceptionCode;

    public Http3ServerCallToObserverAdapter(FrameworkModel frameworkModel, Http3StreamChannel http3StreamChannel) {
        super(frameworkModel, http3StreamChannel);
        setHeadersCustomizer(headers -> {
            if (exceptionCode != 0) {
                headers.set(TripleHeaderEnum.TRI_EXCEPTION_CODE.getHeader(), String.valueOf(exceptionCode));
            }
        });
    }

    @Override
    public void setExceptionCode(int exceptionCode) {
        this.exceptionCode = exceptionCode;
    }
}
