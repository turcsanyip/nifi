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

package org.apache.nifi.snowflake.service;

import java.net.MalformedURLException;
import java.net.URL;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.spec.InvalidKeySpecException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import net.snowflake.ingest.SimpleIngestManager;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.key.service.api.PrivateKeyService;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.snowflake.SnowflakeConfiguration;
import org.apache.nifi.processors.snowflake.SnowflakeConfigurationService;
import org.apache.nifi.processors.snowflake.SnowflakeIngestManagerProviderService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.snowflake.service.util.AccountIdentifierFormat;
import org.apache.nifi.snowflake.service.util.AccountIdentifierFormatParameters;
import org.apache.nifi.processors.snowflake.util.SnowflakeProperties;
import org.apache.nifi.snowflake.service.util.SnowflakeConstants;

@Tags({"snowflake", "snowpipe", "ingest"})
@CapabilityDescription("Provides a Snowflake Ingest Manager for Snowflake pipe processors")
public class StandardSnowflakeIngestManagerProviderService extends AbstractControllerService
        implements SnowflakeIngestManagerProviderService {

    public static final PropertyDescriptor USE_SNOWFLAKE_CONFIGURATION_SERVICE = new PropertyDescriptor.Builder()
            .name("use-snowflake-configuration-service")
            .displayName("Use Snowflake Configuration Service")
            .description("Use Snowflake Configuration Service or configure the properties on the Processor")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor SNOWFLAKE_CONFIGURATION_SERVICE = new PropertyDescriptor.Builder()
            .name("snowflake-configuration-service")
            .displayName("Snowflake Configuration Service")
            .description("The Snowflake Configuration Service for configuring the Snowflake account and user details.")
            .identifiesControllerService(SnowflakeConfigurationService.class)
            .required(true)
            .dependsOn(USE_SNOWFLAKE_CONFIGURATION_SERVICE, "true")
            .build();

    public static final PropertyDescriptor ACCOUNT_IDENTIFIER_FORMAT = new PropertyDescriptor.Builder()
            .name("account-identifier-format")
            .displayName("Account Identifier Format")
            .description("The format of the account identifier.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(true)
            .allowableValues(AccountIdentifierFormat.class)
            .defaultValue(AccountIdentifierFormat.ACCOUNT_NAME.getValue())
            .dependsOn(USE_SNOWFLAKE_CONFIGURATION_SERVICE, "false")
            .build();

    public static final PropertyDescriptor ACCOUNT_URL = new PropertyDescriptor.Builder()
            .name("host-url")
            .displayName("Account URL")
            .description("The Snowflake account URL or hostname (in this case https with default port is assumed). " +
                    "Example: [account-locator].[cloud-region].[cloud]" + SnowflakeConstants.SNOWFLAKE_HOST_SUFFIX)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(true)
            .dependsOn(ACCOUNT_IDENTIFIER_FORMAT, AccountIdentifierFormat.ACCOUNT_URL)
            .build();

    public static final PropertyDescriptor ACCOUNT_LOCATOR = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(SnowflakeProperties.ACCOUNT_LOCATOR)
            .dependsOn(ACCOUNT_IDENTIFIER_FORMAT, AccountIdentifierFormat.ACCOUNT_LOCATOR)
            .build();

    public static final PropertyDescriptor CLOUD_REGION = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(SnowflakeProperties.CLOUD_REGION)
            .dependsOn(ACCOUNT_IDENTIFIER_FORMAT, AccountIdentifierFormat.ACCOUNT_LOCATOR)
            .build();

    public static final PropertyDescriptor CLOUD_TYPE = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(SnowflakeProperties.CLOUD_TYPE)
            .dependsOn(ACCOUNT_IDENTIFIER_FORMAT, AccountIdentifierFormat.ACCOUNT_LOCATOR)
            .build();

    public static final PropertyDescriptor ORGANIZATION_NAME = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(SnowflakeProperties.ORGANIZATION_NAME)
            .dependsOn(ACCOUNT_IDENTIFIER_FORMAT, AccountIdentifierFormat.ACCOUNT_NAME)
            .build();

    public static final PropertyDescriptor ACCOUNT_NAME = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(SnowflakeProperties.ACCOUNT_NAME)
            .dependsOn(ACCOUNT_IDENTIFIER_FORMAT, AccountIdentifierFormat.ACCOUNT_NAME)
            .build();

    public static final PropertyDescriptor USER_NAME = new PropertyDescriptor.Builder()
            .name("user-name")
            .displayName("User Name")
            .description("The Snowflake user name.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(true)
            .dependsOn(USE_SNOWFLAKE_CONFIGURATION_SERVICE, "false")
            .build();

    public static final PropertyDescriptor PRIVATE_KEY_SERVICE = new PropertyDescriptor.Builder()
            .name("private-key-service")
            .displayName("Private Key Service")
            .description("Specifies the Controller Service that will provide the private key. The public key needs to be added to the user account in the Snowflake account beforehand.")
            .identifiesControllerService(PrivateKeyService.class)
            .required(true)
            .dependsOn(USE_SNOWFLAKE_CONFIGURATION_SERVICE, "false")
            .build();

    public static final PropertyDescriptor DATABASE = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(SnowflakeProperties.DATABASE)
            .required(true)
            .build();

    public static final PropertyDescriptor SCHEMA = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(SnowflakeProperties.SCHEMA)
            .required(true)
            .build();

    public static final PropertyDescriptor PIPE = new PropertyDescriptor.Builder()
            .name("pipe")
            .displayName("Pipe")
            .description("The Snowflake pipe to ingest from.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(true)
            .build();

    static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(Arrays.asList(
            USE_SNOWFLAKE_CONFIGURATION_SERVICE,
            SNOWFLAKE_CONFIGURATION_SERVICE,
            ACCOUNT_IDENTIFIER_FORMAT,
            ACCOUNT_URL,
            ACCOUNT_LOCATOR,
            CLOUD_REGION,
            CLOUD_TYPE,
            ORGANIZATION_NAME,
            ACCOUNT_NAME,
            USER_NAME,
            PRIVATE_KEY_SERVICE,
            DATABASE,
            SCHEMA,
            PIPE
    ));

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    private volatile String fullyQualifiedPipeName;
    private volatile SimpleIngestManager ingestManager;

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
        final String database = context.getProperty(DATABASE)
                .evaluateAttributeExpressions()
                .getValue();
        final String schema = context.getProperty(SCHEMA)
                .evaluateAttributeExpressions()
                .getValue();
        final String pipe = context.getProperty(PIPE).evaluateAttributeExpressions().getValue();
        fullyQualifiedPipeName = database + "." + schema + "." + pipe;

        final String user;
        final PrivateKey privateKey;
        final String account;
        final String accountUrl;

        final SnowflakeConfigurationService configurationService = context.getProperty(SNOWFLAKE_CONFIGURATION_SERVICE).asControllerService(SnowflakeConfigurationService.class);
        if (configurationService != null) {
            final SnowflakeConfiguration configuration = configurationService.getConfiguration();
            user = configuration.getUsername();
            privateKey = configuration.getPrivateKey();
            account = configuration.getAccount();
            accountUrl = configuration.getAccountUrl();
        } else {
            user = context.getProperty(USER_NAME).evaluateAttributeExpressions().getValue();

            final PrivateKeyService privateKeyService = context.getProperty(PRIVATE_KEY_SERVICE).asControllerService(PrivateKeyService.class);
            privateKey = privateKeyService.getPrivateKey();

            final AccountIdentifierFormat accountIdentifierFormat = AccountIdentifierFormat.forName(context.getProperty(ACCOUNT_IDENTIFIER_FORMAT).getValue());
            final AccountIdentifierFormatParameters parameters = getAccountIdentifierFormatParameters(context);
            account = accountIdentifierFormat.getAccount(parameters);
            accountUrl = accountIdentifierFormat.getAccountUrl(parameters);
        }

        URL url = null;
        try {
            url = new URL(accountUrl);
        } catch (MalformedURLException e) {
            // url parse failure => handle accountUrl as hostname only
        }

        final String scheme = url != null ? url.getProtocol() : SnowflakeConstants.DEFAULT_SCHEME;
        final String host = url != null ? url.getHost() : accountUrl;
        final int port = url != null && url.getPort() != -1 ? url.getPort() : SnowflakeConstants.DEFAULT_PORT;

        try {
            ingestManager = new SimpleIngestManager(account, user, fullyQualifiedPipeName, privateKey, scheme, host, port);
        } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
            throw new InitializationException("Failed create Snowflake ingest manager", e);
        }
    }

    @OnDisabled
    public void onDisabled() {
        if (ingestManager != null) {
            ingestManager.close();
            ingestManager = null;
        }
    }

    @Override
    public String getPipeName() {
        return fullyQualifiedPipeName;
    }

    @Override
    public SimpleIngestManager getIngestManager() {
        return ingestManager;
    }

    private AccountIdentifierFormatParameters getAccountIdentifierFormatParameters(ConfigurationContext context) {
        final String accountUrl = context.getProperty(ACCOUNT_URL)
                .evaluateAttributeExpressions()
                .getValue();
        final String organizationName = context.getProperty(ORGANIZATION_NAME)
                .evaluateAttributeExpressions()
                .getValue();
        final String accountName = context.getProperty(ACCOUNT_NAME)
                .evaluateAttributeExpressions()
                .getValue();
        final String accountLocator = context.getProperty(ACCOUNT_LOCATOR)
                .evaluateAttributeExpressions()
                .getValue();
        final String cloudRegion = context.getProperty(CLOUD_REGION)
                .evaluateAttributeExpressions()
                .getValue();
        final String cloudType = context.getProperty(CLOUD_TYPE)
                .evaluateAttributeExpressions()
                .getValue();
        return new AccountIdentifierFormatParameters(accountUrl,
                organizationName,
                accountName,
                accountLocator,
                cloudRegion,
                cloudType);
    }
}
