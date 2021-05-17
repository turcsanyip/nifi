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
package org.apache.nifi.processors.grpc;

import com.google.common.collect.Sets;
import com.google.protobuf.Message;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.nifi.annotation.behavior.DynamicProperties;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.grpc.compiler.ProtoCompiler;
import org.apache.nifi.processors.grpc.compiler.ProtoDirectories;
import org.apache.nifi.processors.grpc.compiler.ProtoUtils;
import org.apache.nifi.processors.grpc.util.ProtobufProcessorUtils;
import org.apache.nifi.util.file.classloader.ClassLoaderUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.protobuf.Descriptors.Descriptor;
import static org.apache.nifi.processors.grpc.ConvertProtobuf.MESSAGE_FIELD_MAPPING_PREFIX;

@Tags({"protobuf"})
@CapabilityDescription("Converts a Protocol Buffers message from binary format. The content of the output FlowFile can be the whole message converted to JSON " +
        "or a field of the message. Individual message fields can also be converted to FlowFile attributes.")
@DynamicProperties({
        @DynamicProperty(name = MESSAGE_FIELD_MAPPING_PREFIX + "<FIELD.PATH>", value = "FlowFile attribute name", description = "The property name specifies the field in the request message " +
                "whose value will be added as a FlowFile attribute with the name specified in the property's value (e.g. " + MESSAGE_FIELD_MAPPING_PREFIX + "customer.name => customerName).",
                expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
public class ConvertProtobuf extends AbstractProcessor {

    public static final String MESSAGE_FIELD_MAPPING_PREFIX = "message$";

    public static final PropertyDescriptor PROP_PROTO_DIRECTORY = new PropertyDescriptor.Builder()
            .name("proto-directory")
            .displayName("Proto Directory")
            .description("Directory containing Protocol Buffers message definition (.proto) file(s).")
            .required(true)
            .addValidator(StandardValidators.createDirectoryExistsValidator(true, false))
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor PROP_MESSAGE_TYPE = new PropertyDescriptor.Builder()
            .name("message-type")
            .displayName("Message Type")
            .description("Fully qualified name of the Protocol Buffers message type including its package (eg. mypackage.MyMessage). " +
                    "Please note it is the message type name from the .proto file, not the name of the generated Java class. " +
                    "The .proto files configured in '" + PROP_PROTO_DIRECTORY.getDisplayName() + "' must contain the definition of this message type.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor PROP_OUTPUT_FIELD = new PropertyDescriptor.Builder()
            .name("output-field")
            .displayName("Output Field")
            .description("Field of the message to output as the FlowFile content (e.g. customer.name). If not specified, the whole message will be converted to JSON " +
                    "and will be set in the FlowFile content. If empty string specified, no FlowFile content will be set. The latter can be used when message fields are extracted to FlowFile " +
                    "attributes instead of content (see dynamic properties).")
            .addValidator(Validator.VALID)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(Arrays.asList(
            PROP_PROTO_DIRECTORY,
            PROP_MESSAGE_TYPE,
            PROP_OUTPUT_FIELD
    ));

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Protocol Buffers message converted successfully.")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Protocol Buffers message failed to be converted.")
            .build();

    public static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(Sets.newHashSet(Arrays.asList(
            REL_SUCCESS,
            REL_FAILURE
    )));

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .description("Mapping of message field to FlowFile attribute.")
                .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
                .dynamic(true)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .build();
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    private volatile ProtoDirectories protoDirectories;
    private volatile Map<String, Class<? extends Message>> messageTypes;

    @OnScheduled
    public void onScheduled(ProcessContext context) {
        String protoDirectory = context.getProperty(PROP_PROTO_DIRECTORY).evaluateAttributeExpressions().getValue();

        protoDirectories = ProtoCompiler.compile(protoDirectory, getClass().getSimpleName() + "-" + getIdentifier() + "-");

        List<String> messageClassNames = ProtoUtils.getMessageClassNames(protoDirectories);

        ClassLoader classLoader = createClassLoader(protoDirectories);

        messageTypes = getMessageTypes(messageClassNames, classLoader);
    }

    private ClassLoader createClassLoader(ProtoDirectories protoDirectories) {
        String classpath = protoDirectories.getMessagesDir().toString();
        try {
            return ClassLoaderUtils.getCustomClassLoader(classpath, getClass().getClassLoader(), null);
        } catch (Exception e) {
            throw new ProcessException("Unable to create custom ClassLoader for classpath: " + classpath, e);
        }
    }

    private Map<String, Class<? extends Message>> getMessageTypes(List<String> messageClassNames, ClassLoader classLoader) {
        Map<String, Class<? extends Message>> messageTypes = new HashMap<>();

        for (String messageClassName : messageClassNames) {
            try {
                Class<?> messageClass = classLoader.loadClass(messageClassName);
                if (Message.class.isAssignableFrom(messageClass)) {
                    Descriptor descriptor = (Descriptor) MethodUtils.invokeStaticMethod(messageClass, "getDescriptor");
                    String messageType = descriptor.getFullName();

                    messageTypes.put(messageType, (Class<? extends Message>) messageClass);
                }

            } catch (Exception e) {
                throw new ProcessException("Unable to load message class: " + messageClassName, e);
            }
        }

        return messageTypes;
    }

    @OnStopped
    public void onStopped() {
        if (protoDirectories != null) {
            try {
                Files.walk(protoDirectories.getJavaDir())
                        .sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(File::delete);
                protoDirectories = null;
            } catch (IOException e) {
                getLogger().warn("Unable to delete generated files due to {}", new Object[]{e});
            }
        }
        messageTypes = null;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        String messageType = context.getProperty(PROP_MESSAGE_TYPE).evaluateAttributeExpressions(flowFile).getValue();
        String outputField = context.getProperty(PROP_OUTPUT_FIELD).evaluateAttributeExpressions(flowFile).getValue();

        final Map<String, String> fieldToAttributeMapping = ProtobufProcessorUtils.getFieldToAttributeMapping(context, MESSAGE_FIELD_MAPPING_PREFIX);

        Class<? extends Message> messageClass = messageTypes.get(messageType);
        if (messageClass == null) {
            getLogger().error("Message type not found: {}; transferring to failure", messageType);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        try {
            Message message;
            try (InputStream in = session.read(flowFile)) {
                message = (Message) MethodUtils.invokeStaticMethod(messageClass, "parseFrom", in);
            }

            flowFile = ProtobufProcessorUtils.convertMessageToFlowFile(session, flowFile, getLogger(), message, outputField, fieldToAttributeMapping);

            session.getProvenanceReporter().modifyContent(flowFile);
            session.transfer(flowFile, REL_SUCCESS);
        } catch (Exception e) {
            getLogger().error("Failed to convert {} from Protocol Buffers to JSON due to {}; transferring to failure", flowFile, e);
            session.transfer(flowFile, REL_FAILURE);
        }
    }
}
