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
package org.apache.nifi.services.azure.storage;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;
import org.apache.nifi.security.util.KeyStoreUtils;
import org.apache.nifi.security.util.KeystoreType;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * Provides credentials details for ADLS
 *
 * @see AbstractControllerService
 */
@Tags({"azure", "microsoft", "cloud", "storage", "adls", "credentials"})
@CapabilityDescription("Defines credentials for ADLS processors.")
public class ADLSCredentialsControllerService extends AbstractControllerService implements ADLSCredentialsService {

    public static final PropertyDescriptor ACCOUNT_NAME = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(AzureStorageUtils.ACCOUNT_NAME)
            .description(AzureStorageUtils.ACCOUNT_NAME_BASE_DESCRIPTION)
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    public static final PropertyDescriptor ENDPOINT_SUFFIX = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(AzureStorageUtils.ENDPOINT_SUFFIX)
            .displayName("Endpoint Suffix")
            .description("Storage accounts in public Azure always use a common FQDN suffix. " +
                    "Override this endpoint suffix with a different suffix in certain circumstances (like Azure Stack or non-public Azure regions).")
            .required(true)
            .defaultValue("dfs.core.windows.net")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor ACCOUNT_KEY = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(AzureStorageUtils.ACCOUNT_KEY)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    public static final PropertyDescriptor SAS_TOKEN = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(AzureStorageUtils.PROP_SAS_TOKEN)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    public static final PropertyDescriptor USE_MANAGED_IDENTITY = new PropertyDescriptor.Builder()
            .name("storage-use-managed-identity")
            .displayName("Use Azure Managed Identity")
            .description("Choose whether or not to use the managed identity of Azure VM/VMSS ")
            .required(false)
            .defaultValue("false")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor SERVICE_PRINCIPAL_TENANT_ID = new PropertyDescriptor.Builder()
            .name("service-principal-tenant-id")
            .displayName("Service Principal Tenant ID")
            .description("Tenant ID of the Azure Active Directory hosting the Service Principal. The property is required when Service Principal authentication is used.")
            .sensitive(true)
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    public static final PropertyDescriptor SERVICE_PRINCIPAL_CLIENT_ID = new PropertyDescriptor.Builder()
            .name("service-principal-client-id")
            .displayName("Service Principal Client ID")
            .description("Client ID (or Application ID) of the Client/Application having the Service Principal. The property is required when Service Principal authentication is used. " +
                    "Also 'Service Principal Client Secret' or 'Service Principal Client Certificate Path' along with 'Service Principal Client Certificate Password' must be specified in this case.")
            .sensitive(true)
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    public static final PropertyDescriptor SERVICE_PRINCIPAL_CLIENT_SECRET = new PropertyDescriptor.Builder()
            .name("service-principal-client-secret")
            .displayName("Service Principal Client Secret")
            .description("Password of the Client/Application.")
            .sensitive(true)
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    public static final PropertyDescriptor SERVICE_PRINCIPAL_CLIENT_CERTIFICATE_PATH = new PropertyDescriptor.Builder()
            .name("service-principal-client-certificate-path")
            .displayName("Service Principal Client Certificate Path")
            .description("The path of the keystore containing the client certificate of the Client/Application. Only PKCS12 (.pfx) keystore type is supported. " +
                    "The keystore must contain a single key and the password of the keystore and the key must be the same.")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor SERVICE_PRINCIPAL_CLIENT_CERTIFICATE_PASSWORD = new PropertyDescriptor.Builder()
            .name("service-principal-client-certificate-password")
            .displayName("Service Principal Client Certificate Password")
            .description("The password of the keystore containing the client certificate of the Client/Application.")
            .sensitive(true)
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(Arrays.asList(
            ACCOUNT_NAME,
            ENDPOINT_SUFFIX,
            ACCOUNT_KEY,
            SAS_TOKEN,
            USE_MANAGED_IDENTITY,
            SERVICE_PRINCIPAL_TENANT_ID,
            SERVICE_PRINCIPAL_CLIENT_ID,
            SERVICE_PRINCIPAL_CLIENT_SECRET,
            SERVICE_PRINCIPAL_CLIENT_CERTIFICATE_PATH,
            SERVICE_PRINCIPAL_CLIENT_CERTIFICATE_PASSWORD
    ));

    private ConfigurationContext context;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>();

        boolean accountKeySet = StringUtils.isNotBlank(validationContext.getProperty(ACCOUNT_KEY).getValue());
        boolean sasTokenSet = StringUtils.isNotBlank(validationContext.getProperty(SAS_TOKEN).getValue());
        boolean useManagedIdentitySet = validationContext.getProperty(USE_MANAGED_IDENTITY).asBoolean();

        boolean servicePrincipalTenantIdSet = StringUtils.isNotBlank(validationContext.getProperty(SERVICE_PRINCIPAL_TENANT_ID).getValue());
        boolean servicePrincipalClientIdSet = StringUtils.isNotBlank(validationContext.getProperty(SERVICE_PRINCIPAL_CLIENT_ID).getValue());
        boolean servicePrincipalClientSecretSet = StringUtils.isNotBlank(validationContext.getProperty(SERVICE_PRINCIPAL_CLIENT_SECRET).getValue());
        boolean servicePrincipalClientCertificateKeystorePathSet = validationContext.getProperty(SERVICE_PRINCIPAL_CLIENT_CERTIFICATE_PATH).isSet();
        boolean servicePrincipalClientCertificateKeystorePasswordSet = validationContext.getProperty(SERVICE_PRINCIPAL_CLIENT_CERTIFICATE_PASSWORD).isSet();

        boolean servicePrincipalSet = servicePrincipalTenantIdSet || servicePrincipalClientIdSet || servicePrincipalClientSecretSet
                || servicePrincipalClientCertificateKeystorePathSet || servicePrincipalClientCertificateKeystorePasswordSet;

        if (!onlyOneSet(accountKeySet, sasTokenSet, useManagedIdentitySet, servicePrincipalSet)) {
            results.add(new ValidationResult.Builder().subject(this.getClass().getSimpleName())
                    .valid(false)
                    .explanation("one and only one authentication method of [Account Key, SAS Token, Managed Identity, Service Principal] should be used")
                    .build());
        } else if (servicePrincipalSet) {
            String template = "'%s' must be set when Service Principal authentication is being configured";
            if (!servicePrincipalTenantIdSet) {
                results.add(new ValidationResult.Builder().subject(this.getClass().getSimpleName())
                        .valid(false)
                        .explanation(String.format(template, SERVICE_PRINCIPAL_TENANT_ID.getDisplayName()))
                        .build());
            }
            if (!servicePrincipalClientIdSet) {
                results.add(new ValidationResult.Builder().subject(this.getClass().getSimpleName())
                        .valid(false)
                        .explanation(String.format(template, SERVICE_PRINCIPAL_CLIENT_ID.getDisplayName()))
                        .build());
            }

            boolean servicePrincipalClientCertificateSet = servicePrincipalClientCertificateKeystorePathSet || servicePrincipalClientCertificateKeystorePasswordSet;
            if (!onlyOneSet(servicePrincipalClientSecretSet, servicePrincipalClientCertificateSet)) {
                results.add(new ValidationResult.Builder().subject(this.getClass().getSimpleName())
                        .valid(false)
                        .explanation(String.format("eiter '%s' or '%s' along with '%s' (but not both) must be set when Service Principal authentication is being configured",
                                SERVICE_PRINCIPAL_CLIENT_SECRET.getDisplayName(), SERVICE_PRINCIPAL_CLIENT_CERTIFICATE_PATH.getDisplayName(),
                                SERVICE_PRINCIPAL_CLIENT_CERTIFICATE_PASSWORD.getDisplayName()))
                        .build());
            } else if (servicePrincipalClientCertificateSet) {
                if (!servicePrincipalClientCertificateKeystorePathSet || !servicePrincipalClientCertificateKeystorePasswordSet) {
                    results.add(new ValidationResult.Builder().subject(this.getClass().getSimpleName())
                            .valid(false)
                            .explanation(String.format("both '%s' and '%s' must be set when Service Principal authentication is being configured with Client Certificate",
                                    SERVICE_PRINCIPAL_CLIENT_CERTIFICATE_PATH.getDisplayName(), SERVICE_PRINCIPAL_CLIENT_CERTIFICATE_PASSWORD.getDisplayName()))
                            .build());
                } else {
                    String keystorePath = validationContext.getProperty(SERVICE_PRINCIPAL_CLIENT_CERTIFICATE_PATH).evaluateAttributeExpressions().getValue();
                    char[] keystorePassword = validationContext.getProperty(SERVICE_PRINCIPAL_CLIENT_CERTIFICATE_PASSWORD).getValue().toCharArray();

                    try {
                        File keystoreFile = new File(keystorePath);

                        if (!keystoreFile.exists() || !keystoreFile.isFile()) {
                            results.add(new ValidationResult.Builder().subject(this.getClass().getSimpleName())
                                    .valid(false)
                                    .explanation("the specified keystore path does not exist or it is not a file: " + keystorePath)
                                    .build());
                        } else {
                            URL keystoreURL = keystoreFile.toURI().toURL();

                            if (!KeyStoreUtils.isStoreValid(keystoreURL, KeystoreType.PKCS12, keystorePassword)) {
                                results.add(new ValidationResult.Builder().subject(this.getClass().getSimpleName())
                                        .valid(false)
                                        .explanation("the keystore is not of type PKCS12 or the provided password is invalid")
                                        .build());
                            } else if (!KeyStoreUtils.isKeyPasswordCorrect(keystoreURL, KeystoreType.PKCS12, keystorePassword, keystorePassword)) {
                                results.add(new ValidationResult.Builder().subject(this.getClass().getSimpleName())
                                        .valid(false)
                                        .explanation("the key cannot be found or the provided password is invalid")
                                        .build());
                            }
                        }
                    }  catch (Exception e) {
                        results.add(new ValidationResult.Builder().subject(this.getClass().getSimpleName())
                                .valid(false)
                                .explanation("the provided keystore is invalid: " + e.getMessage())
                                .build());
                    }
                }
            }
        }

        return results;
    }

    private boolean onlyOneSet(Boolean... checks) {
        long nrOfSet = Arrays.stream(checks)
            .filter(check -> check)
            .count();

        return nrOfSet == 1;
    }

    @OnEnabled
    public void onEnabled(ConfigurationContext context) {
        this.context = context;
    }

    @Override
    public ADLSCredentialsDetails getCredentialsDetails(Map<String, String> attributes) {
        ADLSCredentialsDetails.Builder credentialsBuilder = ADLSCredentialsDetails.Builder.newBuilder();

        setValue(credentialsBuilder, ACCOUNT_NAME, PropertyValue::getValue, ADLSCredentialsDetails.Builder::setAccountName, attributes);
        setValue(credentialsBuilder, ACCOUNT_KEY, PropertyValue::getValue, ADLSCredentialsDetails.Builder::setAccountKey, attributes);
        setValue(credentialsBuilder, SAS_TOKEN, PropertyValue::getValue, ADLSCredentialsDetails.Builder::setSasToken, attributes);
        setValue(credentialsBuilder, ENDPOINT_SUFFIX, PropertyValue::getValue, ADLSCredentialsDetails.Builder::setEndpointSuffix, attributes);
        setValue(credentialsBuilder, USE_MANAGED_IDENTITY, PropertyValue::asBoolean, ADLSCredentialsDetails.Builder::setUseManagedIdentity, attributes);
        setValue(credentialsBuilder, SERVICE_PRINCIPAL_TENANT_ID, PropertyValue::getValue, ADLSCredentialsDetails.Builder::setServicePrincipalTenantId, attributes);
        setValue(credentialsBuilder, SERVICE_PRINCIPAL_CLIENT_ID, PropertyValue::getValue, ADLSCredentialsDetails.Builder::setServicePrincipalClientId, attributes);
        setValue(credentialsBuilder, SERVICE_PRINCIPAL_CLIENT_SECRET, PropertyValue::getValue, ADLSCredentialsDetails.Builder::setServicePrincipalClientSecret, attributes);
        setValue(credentialsBuilder, SERVICE_PRINCIPAL_CLIENT_CERTIFICATE_PATH, PropertyValue::getValue, ADLSCredentialsDetails.Builder::setServicePrincipalClientCertificatePath, attributes);
        setValue(credentialsBuilder, SERVICE_PRINCIPAL_CLIENT_CERTIFICATE_PASSWORD, PropertyValue::getValue, ADLSCredentialsDetails.Builder::setServicePrincipalClientCertificatePassword, attributes);

        return credentialsBuilder.build();
    }

    private <T> void setValue(
            ADLSCredentialsDetails.Builder credentialsBuilder,
            PropertyDescriptor propertyDescriptor, Function<PropertyValue, T> getPropertyValue,
            BiConsumer<ADLSCredentialsDetails.Builder, T> setBuilderValue, Map<String, String> attributes
    ) {
        PropertyValue property = context.getProperty(propertyDescriptor);

        if (property.isSet()) {
            if (propertyDescriptor.isExpressionLanguageSupported()) {
                property = property.evaluateAttributeExpressions(attributes);
            }
            T value = getPropertyValue.apply(property);
            setBuilderValue.accept(credentialsBuilder, value);
        }
    }
}
