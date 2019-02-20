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
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonString;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Tags({"post", "slack", "notify"})
@CapabilityDescription("Sends a message to your team on slack.com. The FlowFile content (eg. an image)" +
        " can be attached to the message.")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
public class PostSlack extends AbstractProcessor {

    private static final String SLACK_POST_MESSAGE_URL = "https://slack.com/api/chat.postMessage";

    private static final String SLACK_FILE_UPLOAD_URL = "https://slack.com/api/files.upload";

    public static final PropertyDescriptor POST_MESSAGE_URL = new PropertyDescriptor.Builder()
            .name("post-message-url")
            .displayName("Slack Post Message URL")
            .description("Slack Web API URL for posting text messages to channels. It only needs to be changed" +
                    " if Slack changes its API URL.")
            .required(true)
            .defaultValue(SLACK_POST_MESSAGE_URL)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    public static final PropertyDescriptor FILE_UPLOAD_URL = new PropertyDescriptor.Builder()
            .name("file-upload-url")
            .displayName("Slack File Upload URL")
            .description("Slack Web API URL for uploading files to channels. It only needs to be changed" +
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
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor CHANNEL = new PropertyDescriptor.Builder()
            .name("channel")
            .displayName("Channel")
            .description("Channel, private group, or IM channel to send message to.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor TEXT = new PropertyDescriptor.Builder()
            .name("text")
            .displayName("Text")
            .description("Text of the message to send.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final AllowableValue SEND_FLOWFILE_YES = new AllowableValue(
            "true",
            "Yes",
            "Send FlowFile content in the message."
    );

    public static final AllowableValue SEND_FLOWFILE_NO = new AllowableValue(
            "false",
            "No",
            "Don't send FlowFile content in the message."
    );

    public static final PropertyDescriptor SEND_FLOWFILE = new PropertyDescriptor.Builder()
            .name("send-flowfile")
            .displayName("Send FlowFile")
            .description("Whether or not to send the FlowFile content in the message.")
            .allowableValues(SEND_FLOWFILE_YES, SEND_FLOWFILE_NO)
            .required(true)
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor FILE_NAME = new PropertyDescriptor.Builder()
            .name("file-name")
            .displayName("File Name")
            .description("Name of the file")
            .defaultValue("${" + CoreAttributes.FILENAME.key() + "}")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor FILE_MIME_TYPE = new PropertyDescriptor.Builder()
            .name("file-mime-type")
            .displayName("File Mime Type")
            .description("Mime type of the file")
            .defaultValue("${" + CoreAttributes.MIME_TYPE.key() + "}")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor FILE_TITLE = new PropertyDescriptor.Builder()
            .name("file-title")
            .displayName("File Title")
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
            Arrays.asList(POST_MESSAGE_URL, FILE_UPLOAD_URL, ACCESS_TOKEN, CHANNEL, TEXT, SEND_FLOWFILE, FILE_NAME, FILE_MIME_TYPE, FILE_TITLE));

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
        if (flowFile == null) {
            return;
        }

        try {
            String url;
            String contentType;
            HttpEntity requestBody;

            if (!context.getProperty(SEND_FLOWFILE).asBoolean()) {
                url = context.getProperty(POST_MESSAGE_URL).getValue();
                contentType = ContentType.APPLICATION_JSON.toString();
                requestBody = createTextMessageRequestBody(context, flowFile);
            } else {
                url = context.getProperty(FILE_UPLOAD_URL).getValue();
                contentType = null; // it will be set
                requestBody = createFileMessageRequestBody(context, session, flowFile);
            }

            HttpPost request = new HttpPost(url);
            request.setHeader(HttpHeaders.AUTHORIZATION, "Bearer " + context.getProperty(ACCESS_TOKEN).getValue());
            if (contentType != null) {
                request.setHeader(HttpHeaders.CONTENT_TYPE, contentType);
            }
            request.setEntity(requestBody);

            CloseableHttpClient client = HttpClientBuilder.create().build();
            CloseableHttpResponse response = client.execute(request);

            int statusCode = response.getStatusLine().getStatusCode();
            getLogger().debug("Status code: " + statusCode);

            if (!(statusCode >= 200 && statusCode < 300)) {
                throw new PostSlackException("HTTP error code: " + statusCode);
            }

            JsonObject responseJson = Json.createReader(response.getEntity().getContent()).readObject();

            getLogger().debug("Slack response: " + responseJson.toString());

            if (!responseJson.getBoolean("ok")) {
                throw new PostSlackException("Slack error response: " + responseJson.getString("error"));
            }

            JsonString warning = responseJson.getJsonString("warning");
            if (warning != null) {
                getLogger().warn("Slack warning message: " + warning.getString());
            }

            // TODO: add urls or other important data from the response to the flowfile attributes

            session.transfer(flowFile, REL_SUCCESS);
            session.getProvenanceReporter().send(flowFile, context.getProperty(POST_MESSAGE_URL).getValue());

            // TODO: close resources in finally / try-with-resources
            response.close();
            client.close();
        } catch (IOException | PostSlackException e) {
            getLogger().error("Failed to upload file to Slack", e);
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            context.yield();
        }
    }

    private HttpEntity createTextMessageRequestBody(ProcessContext context, FlowFile flowFile) throws UnsupportedEncodingException {
        JsonObjectBuilder jsonBuilder = Json.createObjectBuilder();

        String channel = context.getProperty(CHANNEL).evaluateAttributeExpressions(flowFile).getValue();
        // handle null / empty
        jsonBuilder.add("channel", channel);

        String text = context.getProperty(TEXT).evaluateAttributeExpressions(flowFile).getValue();
        // handle null / empty
        jsonBuilder.add("text", text);

        return new StringEntity(jsonBuilder.build().toString());
    }

    private HttpEntity createFileMessageRequestBody(ProcessContext context, ProcessSession session, FlowFile flowFile) {
        MultipartEntityBuilder multipartBuilder = MultipartEntityBuilder.create();

        multipartBuilder.addTextBody("channels", context.getProperty(CHANNEL).evaluateAttributeExpressions(flowFile).getValue());

        String text = context.getProperty(TEXT).evaluateAttributeExpressions(flowFile).getValue();
        if (text != null) {
            multipartBuilder.addTextBody("initial_comment", text);
        }

        String title = context.getProperty(FILE_TITLE).evaluateAttributeExpressions(flowFile).getValue();
        if (title != null) {
            multipartBuilder.addTextBody("title", title);
        }

        String fileName = flowFile.getAttribute(CoreAttributes.FILENAME.key());
        if (fileName == null) {
            fileName = "file";
            getLogger().warn("File name not specified, will be set to " + fileName);
        }
        multipartBuilder.addTextBody("filename", fileName);

        ContentType mimeType;
        String mimeTypeStr = flowFile.getAttribute(CoreAttributes.MIME_TYPE.key());
        if (mimeTypeStr == null) {
            mimeType = ContentType.APPLICATION_OCTET_STREAM;
            getLogger().warn("Mime type not specified, will be set to " + mimeType.getMimeType());
        } else {
            mimeType = ContentType.getByMimeType(mimeTypeStr);
            if (mimeType == null) {
                mimeType = ContentType.APPLICATION_OCTET_STREAM;
                getLogger().warn("Unknown mime type specified, will be set to " + mimeType.getMimeType());
            }
        }
        multipartBuilder.addBinaryBody("file", session.read(flowFile), mimeType, fileName);

        return multipartBuilder.build();
    }

    private static class PostSlackException extends Exception {
        PostSlackException(String message) {
            super(message);
        }
    }
}
