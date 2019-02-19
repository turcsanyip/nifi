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

import com.github.seratch.jslack.Slack;
import com.github.seratch.jslack.api.rtm.RTMClient;
import com.github.seratch.jslack.api.rtm.RTMCloseHandler;
import com.github.seratch.jslack.api.rtm.RTMErrorHandler;
import com.github.seratch.jslack.api.rtm.RTMMessageHandler;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.oauth.OAuthAccessTokenService;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

@Tags({"slack, RTM, listen"})
@CapabilityDescription("A Slack bot listening on the Real Time Messaging API.")
public class ListenSlack extends AbstractProcessor {

  private static final PropertyDescriptor API_TOKEN = new PropertyDescriptor.Builder()
    .name("API Token")
    .description("Token for the slack bot user")
    .required(true)
    .identifiesControllerService(OAuthAccessTokenService.class)
    .build();

  private static final PropertyDescriptor MESSAGE_TYPES = new PropertyDescriptor
    .Builder()
    .name("Message types")
    .description("Message types to listen to. It will filter messages with the given types or " +
      "listen to every type if empty. It is a coma separated list like: message,file_shared")
    .required(false)
    .addValidator(Validator.VALID)
    .build();

  private static final Relationship MATCHED_MESSAGES_RELATIONSHIP = new Relationship.Builder()
    .name("matched")
    .description("Incoming messages that matches the given types")
    .build();

  private static final Relationship UNMATCHED_MESSAGES_RELATIONSHIP = new Relationship.Builder()
    .name("unmatched")
    .description("Incoming messages that does not match the given types")
    .autoTerminateDefault(true)
    .build();

  private static final String JSON_OBJECT_TYPE_KEY = "type";

  private List<PropertyDescriptor> descriptors;

  private Set<Relationship> relationships;

  private RTMClient rtm;
  private List<String> matchingTypes;
  private boolean matchAny;

  @Override
  protected void init(final ProcessorInitializationContext context) {
    final List<PropertyDescriptor> descriptors = new ArrayList<>();
    descriptors.add(API_TOKEN);
    descriptors.add(MESSAGE_TYPES);
    this.descriptors = Collections.unmodifiableList(descriptors);

    final Set<Relationship> relationships = new HashSet<>();
    relationships.add(MATCHED_MESSAGES_RELATIONSHIP);
    relationships.add(UNMATCHED_MESSAGES_RELATIONSHIP);
    this.relationships = Collections.unmodifiableSet(relationships);
  }



  @Override
  public Set<Relationship> getRelationships() {
    return this.relationships;
  }

  @Override
  public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
    return descriptors;
  }

  @OnScheduled
  public void onScheduled(final ProcessContext context) {
    String messageTypes = context.getProperty(MESSAGE_TYPES).getValue();
    matchAny = messageTypes.isEmpty();
    matchingTypes = Arrays.asList(messageTypes.split(","));
  }

  @Override
  public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
    ensureClientConnection(context, session);
    context.yield();
  }

  private synchronized void ensureClientConnection(ProcessContext context, ProcessSession session) {
    if (!isRunning()) {
      try {
        rtm = new Slack().rtm(
          context.getProperty(API_TOKEN).asControllerService(OAuthAccessTokenService.class).getAccessToken()
        );
        rtm.addMessageHandler(getMessageHandler(session));
        rtm.addErrorHandler(getErrorHandler());
        rtm.addCloseHandler(getCloseHandler());
        rtm.connect();
      } catch (Exception e) {
        getLogger().error("Error while creating slack client", e);
        closeClient();
        context.yield();
      }
    }
  }

  private RTMCloseHandler getCloseHandler() {
    return closeReason -> {
      getLogger().info("Connection closed. Reason: {}", new Object[]{closeReason});
      closeClient();
    };
  }

  private synchronized void closeClient() {
    rtm = null;
  }

  private RTMErrorHandler getErrorHandler() {
    return throwable -> getLogger().error("Error from slack connection: ", throwable);
  }

  private RTMMessageHandler getMessageHandler(ProcessSession session) {
    return message -> {
      FlowFile flowFile = session.create();
      session.write(flowFile, outputStream -> outputStream.write(message.getBytes()));

      if (matches(message)) {
        session.transfer(flowFile, MATCHED_MESSAGES_RELATIONSHIP);
      } else {
        session.transfer(flowFile, UNMATCHED_MESSAGES_RELATIONSHIP);
      }

      session.commit();
    };
  }

  private boolean matches(String message) {

    if (matchAny) {
      return true;
    }

    JsonReader reader = Json.createReader(new StringReader(message));
    JsonObject jsonObject = reader.readObject();
    String type = jsonObject.getString(JSON_OBJECT_TYPE_KEY);

    return matchingTypes.contains(type);
  }

  private boolean isRunning() {
    return rtm != null;
  }
}
