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
package org.apache.nifi.atlas.provenance.analyzer;

import org.apache.nifi.atlas.provenance.AbstractNiFiProvenanceEventAnalyzer;
import org.apache.nifi.atlas.provenance.AnalysisContext;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class NiFiS2S extends AbstractNiFiProvenanceEventAnalyzer {

    private static final Logger logger = LoggerFactory.getLogger(NiFiS2S.class);

    private static final Pattern RAW_URL_REGEX = Pattern.compile("nifi://([^:/]+):\\d+/([0-9a-zA-Z\\-]+)");
    private static final Pattern HTTP_URL_REGEX = Pattern.compile(".*/nifi-api/data-transfer/(in|out)put-ports/([0-9a-zA-Z\\-]+)/transactions/.*");

    protected URI parseTransitUri(ProvenanceEventRecord event) {
        final String transitUri = event.getTransitUri();

        final URI uri = parseUri(transitUri);
        final String protocol = uri.getScheme();

        switch (protocol) {
            case "http":
            case "https":
                matchUrl(transitUri, HTTP_URL_REGEX);
                break;

            case "nifi":
                matchUrl(transitUri, RAW_URL_REGEX);
                break;

            default:
                throw new IllegalArgumentException("Protocol " + protocol + " is not supported as NiFi S2S Transit URI.");
        }

        return uri;
    }

    protected String getNamespace(AnalysisContext context, URI uri) {
        return context.getNamespaceResolver().fromHostNames(uri.getHost());
    }

    private void matchUrl(String url, Pattern pattern) {
        final Matcher uriMatcher = pattern.matcher(url);
        if (!uriMatcher.matches()) {
            throw new IllegalArgumentException("Unexpected Transit URI: " + url);
        }
    }

}
