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

package org.apache.nifi.snowflake.service.util;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.nifi.components.DescribedValue;

public enum AccountIdentifierFormat implements DescribedValue {
    ACCOUNT_URL("full-url", "Account URL", "Provide an account identifier in a single property") {
        @Override
        public String getAccount(final AccountIdentifierFormatParameters parameters) {
            final String accountUrl = getAccountUrl(parameters);
            String hostname;
            try {
                final URL url = new URL(accountUrl);
                hostname = url.getHost();
            } catch (MalformedURLException e) {
                hostname = accountUrl;
            }
            final String[] hostParts = hostname.split("\\.");
            if (hostParts.length == 0) {
                throw new IllegalArgumentException("Invalid Snowflake host url");
            }
            return hostParts[0];
        }

        @Override
        public String getAccountUrl(final AccountIdentifierFormatParameters parameters) {
            return Objects.requireNonNull(parameters.getAccountUrl());
        }
    },
    ACCOUNT_NAME("account-name", "Account Name", "Provide a Snowflake Account Name") {
        @Override
        public String getAccount(final AccountIdentifierFormatParameters parameters) {
            final String organizationName = Objects.requireNonNull(parameters.getOrganizationName());
            final String accountName = Objects.requireNonNull(parameters.getAccountName());
            return organizationName + "-" + accountName;
        }

        @Override
        public String getAccountUrl(final AccountIdentifierFormatParameters parameters) {
            return getAccount(parameters) + SnowflakeConstants.SNOWFLAKE_HOST_SUFFIX;
        }
    },
    ACCOUNT_LOCATOR("account-locator", "Account Locator", "Provide a Snowflake Account Locator") {
        @Override
        public String getAccount(final AccountIdentifierFormatParameters parameters) {
            return Objects.requireNonNull(parameters.getAccountLocator());
        }

        @Override
        public String getAccountUrl(final AccountIdentifierFormatParameters parameters) {
            final String accountLocator = Objects.requireNonNull(parameters.getAccountLocator());
            final String cloudRegion = Objects.requireNonNull(parameters.getCloudRegion());
            final String optCloudType = parameters.getCloudType();
            final StringBuilder hostBuilder = new StringBuilder();
            hostBuilder.append(accountLocator)
                    .append(".").append(cloudRegion);
            Optional.ofNullable(optCloudType)
                    .ifPresent(cloudType -> hostBuilder.append(".").append(cloudType));
            hostBuilder.append(SnowflakeConstants.SNOWFLAKE_HOST_SUFFIX);
            return hostBuilder.toString();
        }
    };

    private final String value;
    private final String displayName;
    private final String description;

    AccountIdentifierFormat(final String value, final String displayName, final String description) {
        this.value = value;
        this.displayName = displayName;
        this.description = description;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public String getDisplayName() {
        return displayName;
    }

    @Override
    public String getDescription() {
        return description;
    }

    public abstract String getAccount(final AccountIdentifierFormatParameters parameters);
    public abstract String getAccountUrl(final AccountIdentifierFormatParameters parameters);

    public static AccountIdentifierFormat forName(String provideMethod) {
        return Stream.of(values()).filter(provider -> provider.getValue().equalsIgnoreCase(provideMethod))
                .findFirst()
                .orElseThrow(
                        () -> new IllegalArgumentException("Invalid AccountIdentifierFormat: " + provideMethod));
    }
}
