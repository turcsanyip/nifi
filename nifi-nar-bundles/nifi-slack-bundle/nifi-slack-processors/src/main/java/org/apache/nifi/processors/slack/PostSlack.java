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
package org.apache.nifi.processors.slack;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.oauth.OAuthAccessTokenService;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import javax.json.Json;
import javax.json.JsonObject;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Tags({"post", "slack", "notify"})
@CapabilityDescription("Sends a message with the FlowFile as attachment to your team on slack.com")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
public class PostSlack extends AbstractProcessor {

    private static final String SLACK_FILE_UPLOAD_URL = "https://slack.com/api/files.upload";

    public static final PropertyDescriptor FILE_UPLOAD_URL = new PropertyDescriptor.Builder()
            .name("file-upload-url")
            .displayName("Slack Web API file upload URL")
            .description("The POST URL provided by Slack Web API to upload files to channel(s). It only needs to be changed" +
                    " if Slack changes its API URL.")
            .required(true)
            .defaultValue(SLACK_FILE_UPLOAD_URL)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    public static final PropertyDescriptor ACCESS_TOKEN = new PropertyDescriptor.Builder()
            .name("access-token")
            .displayName("Slack Access Token")
            .description("Authentication token")
            .required(true)
            .identifiesControllerService(OAuthAccessTokenService.class)
            .build();

    public static final PropertyDescriptor CHANNELS = new PropertyDescriptor.Builder()
            .name("channels")
            .displayName("Channels")
            .description("Comma-separated list of channel names or IDs where the file will be shared.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor FILENAME = new PropertyDescriptor.Builder()
            .name("filename")
            .displayName("Filename")
            .description("Name of the file")
            .required(true)
            .defaultValue("${" + CoreAttributes.FILENAME.key() + "}")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor MIME_TYPE = new PropertyDescriptor.Builder()
            .name("mime-type")
            .displayName("Mime Type")
            .description("Mime type of the file")
            .required(true)
            .defaultValue("${" + CoreAttributes.MIME_TYPE.key() + "}")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor COMMENT = new PropertyDescriptor.Builder()
            .name("comment")
            .displayName("Comment")
            .description("The message text introducing the file")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor TITLE = new PropertyDescriptor.Builder()
            .name("title")
            .displayName("Title")
            .description("Title of the file")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles are routed to success after being successfully sent to Slack")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles are routed to failure if unable to be sent to Slack")
            .build();

    public static final List<PropertyDescriptor> properties = Collections.unmodifiableList(
            Arrays.asList(FILE_UPLOAD_URL, ACCESS_TOKEN, CHANNELS, FILENAME, MIME_TYPE, COMMENT, TITLE));

    public static final Set<Relationship> relationships = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList(REL_SUCCESS, REL_FAILURE)));

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }

        try {
            MultipartEntityBuilder multipartBuilder = MultipartEntityBuilder.create();
            
            multipartBuilder.addTextBody("channels", context.getProperty(CHANNELS).evaluateAttributeExpressions(flowFile).getValue());
            
            String comment = context.getProperty(COMMENT).evaluateAttributeExpressions(flowFile).getValue();
            if (comment != null) {
                multipartBuilder.addTextBody("initial_comment", comment);
            }
            
            String title = context.getProperty(TITLE).evaluateAttributeExpressions(flowFile).getValue();
            if (title != null) {
                multipartBuilder.addTextBody("title", title);
            }
            
            String filename = flowFile.getAttribute(CoreAttributes.FILENAME.key());
            if (filename == null) {
                filename = "file";
                getLogger().info("Filename not specified, will be set to " + filename);
            }
            multipartBuilder.addTextBody("filename", filename);

            ContentType mimeType;
            String mimeTypeStr = flowFile.getAttribute(CoreAttributes.MIME_TYPE.key());
            if (mimeTypeStr == null) {
                mimeType = ContentType.APPLICATION_OCTET_STREAM;
                getLogger().info("Mime type not specified, will be set to " + mimeType.getMimeType());
            } else {
                mimeType = ContentType.getByMimeType(mimeTypeStr);
                if (mimeType == null) {
                    mimeType = ContentType.APPLICATION_OCTET_STREAM;
                    getLogger().info("Unknown mime type specified, will be set to " + mimeType.getMimeType());
                }
            }
            multipartBuilder.addBinaryBody("file", session.read(flowFile), mimeType, filename);
            
            
            HttpEntity multipart = multipartBuilder.build();

            HttpUriRequest post = RequestBuilder.post()
                    .setUri(context.getProperty(FILE_UPLOAD_URL).getValue())
                    .setHeader(HttpHeaders.AUTHORIZATION, "Bearer " +
                            context.getProperty(ACCESS_TOKEN).asControllerService(OAuthAccessTokenService.class).getAccessToken())
                    .setEntity(multipart)
                    .build();

            CloseableHttpClient client = HttpClientBuilder.create().build();

            CloseableHttpResponse response = client.execute(post);

            int statusCode = response.getStatusLine().getStatusCode();
            getLogger().info("Status code: " + statusCode);

            if (!(statusCode >= 200 && statusCode < 300)) {
                throw new SlackException("HTTP error code: " + statusCode);
            }

            JsonObject responseJson = Json.createReader(response.getEntity().getContent()).readObject();

            getLogger().info("Slack response: " + responseJson.toString());

            if (!responseJson.getBoolean("ok")) {
                throw new SlackException("Slack error response: " + responseJson.getString("error"));
            }

            // TODO: log warnings coming in the slack response

            // TODO: add urls or other important data from the response to the flowfile attributes

            session.transfer(flowFile, REL_SUCCESS);
            session.getProvenanceReporter().send(flowFile, context.getProperty(FILE_UPLOAD_URL).evaluateAttributeExpressions(flowFile).getValue());

            // TODO: close resources in finally / try-with-resources
            response.close();
            client.close();
        } catch (IOException | SlackException e) {
            getLogger().error("Failed to upload file to Slack", e);
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            context.yield();
        }
    }

    private static class SlackException extends Exception {
        SlackException(String message) {
            super(message);
        }
    }
}
