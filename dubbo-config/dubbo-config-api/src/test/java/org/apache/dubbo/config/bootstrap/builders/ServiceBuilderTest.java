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
package org.apache.dubbo.config.bootstrap.builders;

import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.config.ApplicationConfig;
import org.apache.dubbo.config.MethodConfig;
import org.apache.dubbo.config.ProtocolConfig;
import org.apache.dubbo.config.ProviderConfig;
import org.apache.dubbo.config.RegistryConfig;
import org.apache.dubbo.config.ServiceConfig;

import java.util.Collections;

import org.apache.dubbo.config.bootstrap.DubboBootstrap;
import org.apache.dubbo.config.utils.service.FooService;

import org.apache.dubbo.config.utils.service.FooServiceImpl;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.apache.dubbo.common.constants.CommonConstants.GENERIC_SERIALIZATION_BEAN;
import static org.apache.dubbo.common.constants.CommonConstants.GENERIC_SERIALIZATION_DEFAULT;
import static org.apache.dubbo.common.constants.CommonConstants.GENERIC_SERIALIZATION_NATIVE_JAVA;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

class ServiceBuilderTest {

    @Test
    void path() {
        ServiceBuilder builder = new ServiceBuilder();
        builder.path("path");
        Assertions.assertEquals("path", builder.build().getPath());
    }

    @Test
    void addMethod() {
        MethodConfig method = new MethodConfig();
        ServiceBuilder builder = new ServiceBuilder();
        builder.addMethod(method);
        Assertions.assertTrue(builder.build().getMethods().contains(method));
        Assertions.assertEquals(1, builder.build().getMethods().size());
    }

    @Test
    void addMethods() {
        MethodConfig method = new MethodConfig();
        ServiceBuilder builder = new ServiceBuilder();
        builder.addMethods(Collections.singletonList(method));
        Assertions.assertTrue(builder.build().getMethods().contains(method));
        Assertions.assertEquals(1, builder.build().getMethods().size());
    }

    @Test
    void provider() {
        ProviderConfig provider = new ProviderConfig();
        ServiceBuilder builder = new ServiceBuilder();
        builder.provider(provider);
        Assertions.assertSame(provider, builder.build().getProvider());
    }

    @Test
    void providerIds() {
        ServiceBuilder builder = new ServiceBuilder();
        builder.providerIds("providerIds");
        Assertions.assertEquals("providerIds", builder.build().getProviderIds());
    }

    @Test
    void generic() throws Exception {
        ServiceBuilder builder = new ServiceBuilder();
        builder.generic(GENERIC_SERIALIZATION_DEFAULT);
        assertThat(builder.build().getGeneric(), equalTo(GENERIC_SERIALIZATION_DEFAULT));
        builder.generic(GENERIC_SERIALIZATION_NATIVE_JAVA);
        assertThat(builder.build().getGeneric(), equalTo(GENERIC_SERIALIZATION_NATIVE_JAVA));
        builder.generic(GENERIC_SERIALIZATION_BEAN);
        assertThat(builder.build().getGeneric(), equalTo(GENERIC_SERIALIZATION_BEAN));
    }

    @Test
    void generic1() throws Exception {
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            ServiceBuilder builder = new ServiceBuilder();
            builder.generic("illegal").build();
        });
    }
    //
    //    @Test
    //    public void Mock() throws Exception {
    //        Assertions.assertThrows(IllegalArgumentException.class, () -> {
    //            ServiceBuilder builder = new ServiceBuilder();
    //            builder.mock("true");
    //        });
    //    }
    //
    //    @Test
    //    public void Mock1() throws Exception {
    //        Assertions.assertThrows(IllegalArgumentException.class, () -> {
    //            ServiceBuilder builder = new ServiceBuilder();
    //            builder.mock(true);
    //        });
    //    }

    @Test
    void build() {
        MethodConfig method = new MethodConfig();
        ProviderConfig provider = new ProviderConfig();

        ServiceBuilder builder = new ServiceBuilder();
        builder.path("path")
                .addMethod(method)
                .provider(provider)
                .providerIds("providerIds")
                .generic(GENERIC_SERIALIZATION_DEFAULT);

        ServiceConfig config = builder.build();
        ServiceConfig config2 = builder.build();

        assertThat(config.getGeneric(), equalTo(GENERIC_SERIALIZATION_DEFAULT));
        Assertions.assertEquals("path", config.getPath());
        Assertions.assertEquals("providerIds", config.getProviderIds());
        Assertions.assertSame(provider, config.getProvider());
        Assertions.assertTrue(config.getMethods().contains(method));
        Assertions.assertEquals(1, config.getMethods().size());
        Assertions.assertNotSame(config, config2);
    }


        @Test
        public void Mock() throws Exception {
            ServiceConfig<FooService> serviceServiceConfig = new ServiceConfig<>();
            serviceServiceConfig.setInterface(FooService.class);
            serviceServiceConfig.setRef(new FooServiceImpl());
            serviceServiceConfig.setTimeout(10000000);

            DubboBootstrap bootstrap = DubboBootstrap.getInstance();
            ProtocolConfig protocolConfig = new ProtocolConfig(CommonConstants.TRIPLE,8090);
            protocolConfig.setHost("0.0.0.0");
            protocolConfig.setQuicEnabled(true);
            bootstrap.application(new ApplicationConfig("dubbo-demo-triple-api-provider"))
                    .registry(new RegistryConfig("zookeeper://127.0.0.1:2181"))
                    .protocol(protocolConfig)
                    .service(serviceServiceConfig)
                    .start().await();

        }
}
