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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestADLSCredentialsControllerService {

    public static final String CREDENTIALS_SERVICE_IDENTIFIER = "credentials-service";

    private static final String SERVICE_PRINCIPAL_CLIENT_CERTIFICATE_BASE_DIR = "src/test/resources/adls/service-principal/";

    private static final String ACCOUNT_NAME_VALUE = "AccountName";
    private static final String ACCOUNT_KEY_VALUE = "AccountKey";
    private static final String SAS_TOKEN_VALUE = "SasToken";
    private static final String END_POINT_SUFFIX_VALUE = "end.point.suffix";
    private static final String SERVICE_PRINCIPAL_TENANT_ID_VALUE = "ServicePrincipalTenantID";
    private static final String SERVICE_PRINCIPAL_CLIENT_ID_VALUE = "ServicePrincipalClientID";
    private static final String SERVICE_PRINCIPAL_CLIENT_SECRET_VALUE = "ServicePrincipalClientSecret";
    private static final String SERVICE_PRINCIPAL_CLIENT_CERTIFICATE_PATH_VALUE = SERVICE_PRINCIPAL_CLIENT_CERTIFICATE_BASE_DIR + "client-certificate.pfx";
    private static final String SERVICE_PRINCIPAL_CLIENT_CERTIFICATE_PASSWORD_VALUE = "password";

    private TestRunner runner;
    private ADLSCredentialsControllerService credentialsService;

    @Before
    public void setUp() throws InitializationException {
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        credentialsService = new ADLSCredentialsControllerService();
        runner.addControllerService(CREDENTIALS_SERVICE_IDENTIFIER, credentialsService);
    }

    @Test
    public void testNotValidBecauseAccountNameMissing() {
        configureAccountKey();

        runner.assertNotValid(credentialsService);
    }

    @Test
    public void testNotValidBecauseNoCredentialsIsSet() {
        configureAccountName();

        runner.assertNotValid(credentialsService);
    }

    @Test
    public void testNotValidBecauseBothAccountKeyAndSasTokenSpecified() {
        configureAccountName();

        configureAccountKey();
        configureSasToken();

        runner.assertNotValid(credentialsService);
    }

    @Test
    public void testNotValidBecauseBothAccountKeyAndUseManagedIdentitySpecified() {
        configureAccountName();

        configureAccountKey();
        configureUseManagedIdentity();

        runner.assertNotValid(credentialsService);
    }

    @Test
    public void testNotValidBecauseBothAccountKeyAndServicePrincipalTenantIdSpecified() {
        configureAccountName();

        configureAccountKey();
        configureServicePrincipalTenantId();

        runner.assertNotValid(credentialsService);
    }

    @Test
    public void testNotValidBecauseBothAccountKeyAndServicePrincipalClientIdSpecified() {
        configureAccountName();

        configureAccountKey();
        configureServicePrincipalClientId();

        runner.assertNotValid(credentialsService);
    }

    @Test
    public void testNotValidBecauseBothAccountKeyAndServicePrincipalClientSecretSpecified() {
        configureAccountName();

        configureAccountKey();
        configureServicePrincipalClientSecret();

        runner.assertNotValid(credentialsService);
    }

    @Test
    public void testNotValidBecauseBothAccountKeyAndServicePrincipalClientCertificatePathSpecified() {
        configureAccountName();

        configureAccountKey();
        configureServicePrincipalClientCertificatePath();

        runner.assertNotValid(credentialsService);
    }

    @Test
    public void testNotValidBecauseBothAccountKeyAndServicePrincipalClientCertificatePasswordSpecified() {
        configureAccountName();

        configureAccountKey();
        configureServicePrincipalClientCertificatePassword();

        runner.assertNotValid(credentialsService);
    }

    @Test
    public void testNotValidBecauseBothSasTokenAndUseManagedIdentitySpecified() {
        configureAccountName();

        configureSasToken();
        configureUseManagedIdentity();

        runner.assertNotValid(credentialsService);
    }

    @Test
    public void testNotValidBecauseBothSasTokenAndServicePrincipalTenantIdSpecified() {
        configureAccountName();

        configureSasToken();
        configureServicePrincipalTenantId();

        runner.assertNotValid(credentialsService);
    }

    @Test
    public void testNotValidBecauseBothSasTokenAndServicePrincipalClientIdSpecified() {
        configureAccountName();

        configureSasToken();
        configureServicePrincipalClientId();

        runner.assertNotValid(credentialsService);
    }

    @Test
    public void testNotValidBecauseBothSasTokenAndServicePrincipalClientSecretSpecified() {
        configureAccountName();

        configureSasToken();
        configureServicePrincipalClientSecret();

        runner.assertNotValid(credentialsService);
    }

    @Test
    public void testNotValidBecauseBothSasTokenAndServicePrincipalClientCertificatePathSpecified() {
        configureAccountName();

        configureSasToken();
        configureServicePrincipalClientCertificatePath();

        runner.assertNotValid(credentialsService);
    }

    @Test
    public void testNotValidBecauseBothSasTokenAndServicePrincipalClientCertificatePasswordSpecified() {
        configureAccountName();

        configureSasToken();
        configureServicePrincipalClientCertificatePassword();

        runner.assertNotValid(credentialsService);
    }

    @Test
    public void testNotValidBecauseBothUseManagedIdentityAndServicePrincipalTenantIdSpecified() {
        configureAccountName();

        configureUseManagedIdentity();
        configureServicePrincipalTenantId();

        runner.assertNotValid(credentialsService);
    }

    @Test
    public void testNotValidBecauseBothUseManagedIdentityAndServicePrincipalClientIdSpecified() {
        configureAccountName();

        configureUseManagedIdentity();
        configureServicePrincipalClientId();

        runner.assertNotValid(credentialsService);
    }

    @Test
    public void testNotValidBecauseBothUseManagedIdentityAndServicePrincipalClientSecretSpecified() {
        configureAccountName();

        configureUseManagedIdentity();
        configureServicePrincipalClientSecret();

        runner.assertNotValid(credentialsService);
    }

    @Test
    public void testNotValidBecauseBothUseManagedIdentityAndServicePrincipalClientCertificatePathSpecified() {
        configureAccountName();

        configureUseManagedIdentity();
        configureServicePrincipalClientCertificatePath();

        runner.assertNotValid(credentialsService);
    }

    @Test
    public void testNotValidBecauseBothUseManagedIdentityAndServicePrincipalClientCertificatePasswordSpecified() {
        configureAccountName();

        configureUseManagedIdentity();
        configureServicePrincipalClientCertificatePassword();

        runner.assertNotValid(credentialsService);
    }

    @Test
    public void testNotValidBecauseAllCredentialsSpecified() {
        configureAccountName();

        configureAccountKey();
        configureSasToken();
        configureUseManagedIdentity();
        configureServicePrincipalTenantId();
        configureServicePrincipalClientId();
        configureServicePrincipalClientSecret();
        configureServicePrincipalClientCertificatePath();
        configureServicePrincipalClientCertificatePassword();

        runner.assertNotValid(credentialsService);
    }

    @Test
    public void testNotValidWithEmptyEndpointSuffix() {
        configureAccountName();
        configureAccountKey();

        runner.setProperty(credentialsService, ADLSCredentialsControllerService.ENDPOINT_SUFFIX, "");
        runner.assertNotValid(credentialsService);
    }
    @Test
    public void testNotValidWithWhitespaceEndpointSuffix() {
        configureAccountName();
        configureAccountKey();

        runner.setProperty(credentialsService, ADLSCredentialsControllerService.ENDPOINT_SUFFIX, " ");
        runner.assertNotValid(credentialsService);
    }

    @Test
    public void testValidWithAccountNameAndAccountKey() {
        configureAccountName();
        configureAccountKey();

        runner.assertValid(credentialsService);
    }

    @Test
    public void testValidWithAccountNameAndSasToken() {
        configureAccountName();
        configureSasToken();

        runner.assertValid(credentialsService);
    }

    @Test
    public void testValidWithAccountNameAndUseManagedIdentity() {
        configureAccountName();
        configureUseManagedIdentity();

        runner.assertValid(credentialsService);
    }

    @Test
    public void testValidWithAccountNameAndServicePrincipalWithClientSecret() {
        configureAccountName();
        configureServicePrincipalTenantId();
        configureServicePrincipalClientId();
        configureServicePrincipalClientSecret();

        runner.assertValid(credentialsService);
    }

    @Test
    public void testValidWithAccountNameAndServicePrincipalWithClientCertificate() throws Exception {
        configureAccountName();
        configureServicePrincipalTenantId();
        configureServicePrincipalClientId();
        configureServicePrincipalClientCertificatePath();
        configureServicePrincipalClientCertificatePassword();

        runner.assertValid(credentialsService);
    }

    @Test
    public void testNotValidBecauseNoTenantIdSpecifiedForServicePrincipal() {
        configureAccountName();

        configureServicePrincipalClientId();
        configureServicePrincipalClientSecret();

        runner.assertNotValid(credentialsService);
    }

    @Test
    public void testNotValidBecauseNoClientIdSpecifiedForServicePrincipal() {
        configureAccountName();

        configureServicePrincipalTenantId();
        configureServicePrincipalClientSecret();

        runner.assertNotValid(credentialsService);
    }

    @Test
    public void testNotValidBecauseNeitherClientSecretNorClientCertificateSpecifiedForServicePrincipal() {
        configureAccountName();

        configureServicePrincipalTenantId();
        configureServicePrincipalClientId();

        runner.assertNotValid(credentialsService);
    }

    @Test
    public void testNotValidBecauseBothClientSecretAndClientCertificatePathSpecifiedForServicePrincipal() {
        configureAccountName();

        configureServicePrincipalTenantId();
        configureServicePrincipalClientId();
        configureServicePrincipalClientSecret();
        configureServicePrincipalClientCertificatePath();

        runner.assertNotValid(credentialsService);
    }

    @Test
    public void testNotValidBecauseBothClientSecretAndClientCertificatePasswordSpecifiedForServicePrincipal() {
        configureAccountName();

        configureServicePrincipalTenantId();
        configureServicePrincipalClientId();
        configureServicePrincipalClientSecret();
        configureServicePrincipalClientCertificatePassword();

        runner.assertNotValid(credentialsService);
    }

    @Test
    public void testNotValidBecauseNoClientCertificatePathSpecifiedForServicePrincipalClientCertificate() {
        configureAccountName();

        configureServicePrincipalTenantId();
        configureServicePrincipalClientId();
        configureServicePrincipalClientCertificatePassword();

        runner.assertNotValid(credentialsService);
    }

    @Test
    public void testNotValidBecauseNoClientCertificatePasswordSpecifiedForServicePrincipalClientCertificate() {
        configureAccountName();

        configureServicePrincipalTenantId();
        configureServicePrincipalClientId();
        configureServicePrincipalClientCertificatePath();

        runner.assertNotValid(credentialsService);
    }

    @Test
    public void testNotValidBecauseServicePrincipalClientCertificatePathDoesNotExist() {
        configureAccountName();

        configureServicePrincipalTenantId();
        configureServicePrincipalClientId();
        configureServicePrincipalClientCertificatePath(SERVICE_PRINCIPAL_CLIENT_CERTIFICATE_BASE_DIR + "dummy.pfx");
        configureServicePrincipalClientCertificatePassword();

        runner.assertNotValid(credentialsService);
    }

    @Test
    public void testNotValidBecauseServicePrincipalClientCertificatePathIsNotAFile() {
        configureAccountName();

        configureServicePrincipalTenantId();
        configureServicePrincipalClientId();
        configureServicePrincipalClientCertificatePath(SERVICE_PRINCIPAL_CLIENT_CERTIFICATE_BASE_DIR);
        configureServicePrincipalClientCertificatePassword();

        runner.assertNotValid(credentialsService);
    }

    @Test
    public void testNotValidBecauseServicePrincipalClientCertificatePasswordIsWrong() {
        configureAccountName();

        configureServicePrincipalTenantId();
        configureServicePrincipalClientId();
        configureServicePrincipalClientCertificatePath();
        configureServicePrincipalClientCertificatePassword("wrong-password");

        runner.assertNotValid(credentialsService);
    }

    @Test
    public void testNotValidBecauseServicePrincipalClientCertificateKeyNotFound() {
        configureAccountName();

        configureServicePrincipalTenantId();
        configureServicePrincipalClientId();
        configureServicePrincipalClientCertificatePath(SERVICE_PRINCIPAL_CLIENT_CERTIFICATE_BASE_DIR + "client-certificate-no-key.jks");
        configureServicePrincipalClientCertificatePassword();

        runner.assertNotValid(credentialsService);
    }

    @Test
    public void testNotValidBecauseServicePrincipalClientCertificateKeyPasswordIsDifferent() {
        configureAccountName();

        configureServicePrincipalTenantId();
        configureServicePrincipalClientId();
        configureServicePrincipalClientCertificatePath(SERVICE_PRINCIPAL_CLIENT_CERTIFICATE_BASE_DIR + "client-certificate-diff-key-pass.jks");
        configureServicePrincipalClientCertificatePassword();

        runner.assertNotValid(credentialsService);
    }

    @Test
    public void testNotValidBecauseServicePrincipalClientCertificateKeystoreIsJKS() {
        configureAccountName();

        configureServicePrincipalTenantId();
        configureServicePrincipalClientId();
        configureServicePrincipalClientCertificatePath(SERVICE_PRINCIPAL_CLIENT_CERTIFICATE_BASE_DIR + "client-certificate.jks");
        configureServicePrincipalClientCertificatePassword();

        runner.assertNotValid(credentialsService);
    }

    @Test
    public void testGetCredentialsDetailsWithAccountKey() throws Exception {
        // GIVEN
        configureAccountName();
        configureAccountKey();

        runner.enableControllerService(credentialsService);

        // WHEN
        ADLSCredentialsDetails actual = credentialsService.getCredentialsDetails(new HashMap<>());

        // THEN
        assertEquals(ACCOUNT_NAME_VALUE, actual.getAccountName());
        assertEquals(ACCOUNT_KEY_VALUE, actual.getAccountKey());
        assertNull(actual.getSasToken());
        assertFalse(actual.getUseManagedIdentity());
        assertNotNull(actual.getEndpointSuffix());
        assertNull(actual.getServicePrincipalTenantId());
        assertNull(actual.getServicePrincipalClientId());
        assertNull(actual.getServicePrincipalClientSecret());
        assertNull(actual.getServicePrincipalClientCertificatePath());
        assertNull(actual.getServicePrincipalClientCertificatePassword());
    }

    @Test
    public void testGetCredentialsDetailsWithSasToken() throws Exception {
        // GIVEN
        configureAccountName();
        configureSasToken();

        runner.enableControllerService(credentialsService);

        // WHEN
        ADLSCredentialsDetails actual = credentialsService.getCredentialsDetails(new HashMap<>());

        // THEN
        assertEquals(ACCOUNT_NAME_VALUE, actual.getAccountName());
        assertEquals(SAS_TOKEN_VALUE, actual.getSasToken());
        assertNull(actual.getAccountKey());
        assertFalse(actual.getUseManagedIdentity());
        assertNotNull(actual.getEndpointSuffix());
        assertNull(actual.getServicePrincipalTenantId());
        assertNull(actual.getServicePrincipalClientId());
        assertNull(actual.getServicePrincipalClientSecret());
        assertNull(actual.getServicePrincipalClientCertificatePath());
        assertNull(actual.getServicePrincipalClientCertificatePassword());
    }

    @Test
    public void testGetCredentialsDetailsWithUseManagedIdentity() throws Exception {
        // GIVEN
        configureAccountName();
        configureUseManagedIdentity();

        runner.enableControllerService(credentialsService);

        // WHEN
        ADLSCredentialsDetails actual = credentialsService.getCredentialsDetails(new HashMap<>());

        // THEN
        assertEquals(ACCOUNT_NAME_VALUE, actual.getAccountName());
        assertTrue(actual.getUseManagedIdentity());
        assertNull(actual.getAccountKey());
        assertNull(actual.getSasToken());
        assertNotNull(actual.getEndpointSuffix());
        assertNull(actual.getServicePrincipalTenantId());
        assertNull(actual.getServicePrincipalClientId());
        assertNull(actual.getServicePrincipalClientSecret());
        assertNull(actual.getServicePrincipalClientCertificatePath());
        assertNull(actual.getServicePrincipalClientCertificatePassword());
    }

    @Test
    public void testGetCredentialsDetailsWithServicePrincipalWithClientSecret() {
        // GIVEN
        configureAccountName();
        configureServicePrincipalTenantId();
        configureServicePrincipalClientId();
        configureServicePrincipalClientSecret();

        runner.enableControllerService(credentialsService);

        // WHEN
        ADLSCredentialsDetails actual = credentialsService.getCredentialsDetails(new HashMap<>());

        // THEN
        assertEquals(ACCOUNT_NAME_VALUE, actual.getAccountName());
        assertNull(actual.getAccountKey());
        assertNull(actual.getSasToken());
        assertFalse(actual.getUseManagedIdentity());
        assertNotNull(actual.getEndpointSuffix());
        assertEquals(SERVICE_PRINCIPAL_TENANT_ID_VALUE, actual.getServicePrincipalTenantId());
        assertEquals(SERVICE_PRINCIPAL_CLIENT_ID_VALUE, actual.getServicePrincipalClientId());
        assertEquals(SERVICE_PRINCIPAL_CLIENT_SECRET_VALUE, actual.getServicePrincipalClientSecret());
        assertNull(actual.getServicePrincipalClientCertificatePath());
        assertNull(actual.getServicePrincipalClientCertificatePassword());
    }

    @Test
    public void testGetCredentialsDetailsWithServicePrincipalWithClientCertificate() {
        // GIVEN
        configureAccountName();
        configureServicePrincipalTenantId();
        configureServicePrincipalClientId();
        configureServicePrincipalClientCertificatePath();
        configureServicePrincipalClientCertificatePassword();

        executeTestGetCredentialsDetailsWithServicePrincipalWithClientCertificate();
    }

    @Test
    public void testGetCredentialsDetailsWithServicePrincipalWithClientCertificateUsingEL() {
        // GIVEN
        configureAccountName();
        configureServicePrincipalTenantId();
        configureServicePrincipalClientId();
        configureServicePrincipalClientCertificatePathUsingEL();
        configureServicePrincipalClientCertificatePassword();

        executeTestGetCredentialsDetailsWithServicePrincipalWithClientCertificate();
    }

    private void executeTestGetCredentialsDetailsWithServicePrincipalWithClientCertificate() {
        // GIVEN
        runner.enableControllerService(credentialsService);

        // WHEN
        ADLSCredentialsDetails actual = credentialsService.getCredentialsDetails(new HashMap<>());

        // THEN
        assertEquals(ACCOUNT_NAME_VALUE, actual.getAccountName());
        assertNull(actual.getAccountKey());
        assertNull(actual.getSasToken());
        assertFalse(actual.getUseManagedIdentity());
        assertNotNull(actual.getEndpointSuffix());
        assertEquals(SERVICE_PRINCIPAL_TENANT_ID_VALUE, actual.getServicePrincipalTenantId());
        assertEquals(SERVICE_PRINCIPAL_CLIENT_ID_VALUE, actual.getServicePrincipalClientId());
        assertNull(actual.getServicePrincipalClientSecret());
        assertEquals(SERVICE_PRINCIPAL_CLIENT_CERTIFICATE_PATH_VALUE, actual.getServicePrincipalClientCertificatePath());
        assertEquals(SERVICE_PRINCIPAL_CLIENT_CERTIFICATE_PASSWORD_VALUE, actual.getServicePrincipalClientCertificatePassword());
    }

    @Test
    public void testGetCredentialsDetailsWithSetEndpointSuffix() throws Exception {
        // GIVEN
        configureAccountName();
        configureAccountKey();
        configureEndpointSuffix();

        runner.enableControllerService(credentialsService);

        // WHEN
        ADLSCredentialsDetails actual = credentialsService.getCredentialsDetails(new HashMap<>());

        // THEN
        assertEquals(END_POINT_SUFFIX_VALUE, actual.getEndpointSuffix());
    }

    @Test
    public void testGetCredentialsDetailsWithSetEndpointSuffixUsingEL() throws Exception {
        // GIVEN
        configureAccountName();
        configureAccountKey();
        configureEndpointSuffixUsingEL();

        runner.enableControllerService(credentialsService);

        // WHEN
        ADLSCredentialsDetails actual = credentialsService.getCredentialsDetails(new HashMap<>());

        // THEN
        assertEquals(END_POINT_SUFFIX_VALUE, actual.getEndpointSuffix());
    }

    private void configureAccountName() {
        runner.setProperty(credentialsService, ADLSCredentialsControllerService.ACCOUNT_NAME, ACCOUNT_NAME_VALUE);
    }

    private void configureAccountKey() {
        runner.setProperty(credentialsService, ADLSCredentialsControllerService.ACCOUNT_KEY, ACCOUNT_KEY_VALUE);
    }

    private void configureSasToken() {
        runner.setProperty(credentialsService, ADLSCredentialsControllerService.SAS_TOKEN, SAS_TOKEN_VALUE);
    }

    private void configureUseManagedIdentity() {
        runner.setProperty(credentialsService, ADLSCredentialsControllerService.USE_MANAGED_IDENTITY, "true");
    }

    private void configureEndpointSuffix() {
        runner.setProperty(credentialsService, ADLSCredentialsControllerService.ENDPOINT_SUFFIX, END_POINT_SUFFIX_VALUE);
    }

    private void configureEndpointSuffixUsingEL() {
        String variableName = "endpoint.suffix";
        configurePropertyUsingEL(ADLSCredentialsControllerService.ENDPOINT_SUFFIX, variableName, END_POINT_SUFFIX_VALUE);
    }

    private void configureServicePrincipalTenantId() {
        runner.setProperty(credentialsService, ADLSCredentialsControllerService.SERVICE_PRINCIPAL_TENANT_ID, SERVICE_PRINCIPAL_TENANT_ID_VALUE);
    }

    private void configureServicePrincipalClientId() {
        runner.setProperty(credentialsService, ADLSCredentialsControllerService.SERVICE_PRINCIPAL_CLIENT_ID, SERVICE_PRINCIPAL_CLIENT_ID_VALUE);
    }

    private void configureServicePrincipalClientSecret() {
        runner.setProperty(credentialsService, ADLSCredentialsControllerService.SERVICE_PRINCIPAL_CLIENT_SECRET, SERVICE_PRINCIPAL_CLIENT_SECRET_VALUE);
    }

    private void configureServicePrincipalClientCertificatePath() {
        configureServicePrincipalClientCertificatePath(SERVICE_PRINCIPAL_CLIENT_CERTIFICATE_PATH_VALUE);
    }

    private void configureServicePrincipalClientCertificatePath(String path) {
        runner.setProperty(credentialsService, ADLSCredentialsControllerService.SERVICE_PRINCIPAL_CLIENT_CERTIFICATE_PATH, path);
    }

    private void configureServicePrincipalClientCertificatePathUsingEL() {
        String variableName = "service.principal.client.certificate.path";
        configurePropertyUsingEL(ADLSCredentialsControllerService.SERVICE_PRINCIPAL_CLIENT_CERTIFICATE_PATH, variableName, SERVICE_PRINCIPAL_CLIENT_CERTIFICATE_PATH_VALUE);
    }

    private void configureServicePrincipalClientCertificatePassword() {
        configureServicePrincipalClientCertificatePassword(SERVICE_PRINCIPAL_CLIENT_CERTIFICATE_PASSWORD_VALUE);
    }

    private void configureServicePrincipalClientCertificatePassword(String password) {
        runner.setProperty(credentialsService, ADLSCredentialsControllerService.SERVICE_PRINCIPAL_CLIENT_CERTIFICATE_PASSWORD, password);
    }

    private void configurePropertyUsingEL(PropertyDescriptor propertyDescriptor, String variableName, String variableValue) {
        runner.setProperty(credentialsService, propertyDescriptor, String.format("${%s}", variableName));
        runner.setVariable(variableName, variableValue);
    }
}
