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
package org.apache.nifi.processors.aws.signer;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.http.auth.aws.scheme.AwsV4AuthScheme;
import software.amazon.awssdk.http.auth.aws.signer.AwsV4HttpSigner;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.identity.spi.IdentityProvider;
import software.amazon.awssdk.identity.spi.IdentityProviders;

public class CustomSignerSupport {

    public static final PropertyDescriptor CUSTOM_SIGNER_CLASS_NAME = new PropertyDescriptor.Builder()
            .name("Custom Signer Class Name")
            .description(String.format("Fully qualified class name of the custom signer class. The signer must implement %s interface and provide a constructor that accepts a %s.",
                    AwsV4HttpSigner.class.getName(), PropertyContext.class.getName()))
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor CUSTOM_SIGNER_MODULE_LOCATION = new PropertyDescriptor.Builder()
            .name("Custom Signer Module Location")
            .description("Comma-separated list of paths to files and/or directories which contain the custom signer's JAR file and its dependencies (if any).")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .dependsOn(CUSTOM_SIGNER_CLASS_NAME)
            .identifiesExternalResource(ResourceCardinality.MULTIPLE, ResourceType.FILE, ResourceType.DIRECTORY)
            .dynamicallyModifiesClasspath(true)
            .build();

    public static void configureCustomSigner(String signerClassName, PropertyContext context, AwsClientBuilder<?, ?> clientBuilder) {
        final AwsV4HttpSigner signer;

        try {
            signer = Class.forName(signerClassName, true, Thread.currentThread().getContextClassLoader())
                    .asSubclass(AwsV4HttpSigner.class)
                    .getDeclaredConstructor(PropertyContext.class)
                    .newInstance(context);
        } catch (Exception e) {
            throw new ProcessException(String.format("Failed to initialize custom signer [%s]", signerClassName), e);
        }

        clientBuilder.putAuthScheme(new AwsV4AuthScheme() {

            private final AwsV4AuthScheme defaultAuthScheme = AwsV4AuthScheme.create();

            @Override
            public String schemeId() {
                return defaultAuthScheme.schemeId();
            }

            @Override
            public IdentityProvider<AwsCredentialsIdentity> identityProvider(IdentityProviders providers) {
                return defaultAuthScheme.identityProvider(providers);
            }

            @Override
            public AwsV4HttpSigner signer() {
                return signer;
            }
        });
    }
}
