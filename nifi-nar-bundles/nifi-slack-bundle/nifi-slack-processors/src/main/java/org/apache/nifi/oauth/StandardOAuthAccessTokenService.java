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
package org.apache.nifi.oauth;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.Collections;
import java.util.List;

@Tags({"oauth", "authentication", "secure"})
@CapabilityDescription("Standard implementation of OAuthAccessTokenService. Provides the ability to configure OAuth Access Token" +
        " once and reuse it throughout the application")
public class StandardOAuthAccessTokenService extends AbstractControllerService implements OAuthAccessTokenService {

    public static final PropertyDescriptor ACCESS_TOKEN = new PropertyDescriptor.Builder()
            .name("access-token")
            .displayName("OAuth Access Token")
            .description("Authentication token")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();

    private static final List<PropertyDescriptor> properties = Collections.unmodifiableList(
            Collections.singletonList(ACCESS_TOKEN));

    private ConfigurationContext configContext;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        configContext = context;
    }

    @Override
    public String getAccessToken() {
        return configContext.getProperty(ACCESS_TOKEN).getValue();
    }
}
