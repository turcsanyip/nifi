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
package org.apache.nifi.processors.grpc.util;

import com.google.protobuf.Message;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processors.grpc.converter.ProtobufMessageConverter;

import java.io.BufferedOutputStream;
import java.util.HashMap;
import java.util.Map;

public class ProtobufProcessorUtils {

    public static Map<String, String> getFieldToAttributeMapping(ProcessContext context, String prefix) {
        final Map<String, String> requestAttributeFields = new HashMap<>();

        for (PropertyDescriptor propertyDescriptor: context.getProperties().keySet()) {
            if (propertyDescriptor.isDynamic()) {
                String propertyName = propertyDescriptor.getName();
                if (StringUtils.isNotEmpty(prefix) && propertyName.startsWith(prefix)) {
                    String fieldPath = StringUtils.substringAfter(propertyName, prefix);
                    String attributeName = context.getProperty(propertyName).getValue();
                    requestAttributeFields.put(fieldPath, attributeName);
                }
            }
        }

        return requestAttributeFields;
    }

    public static FlowFile convertMessageToFlowFile(ProcessSession session, FlowFile flowFile, ComponentLog componentLogger,
                                                    Message message, String contentField, Map<String, String> fieldToAttributeMapping) {
        byte[] flowFileContent = ProtobufMessageConverter.convertToPayload(message, contentField);

        if (flowFileContent != null) {
            flowFile = session.write(flowFile, out -> {
                try (final BufferedOutputStream bos = new BufferedOutputStream(out, 65536)) {
                    bos.write(flowFileContent);
                    bos.flush();
                }
            });
        }

        Map<String, String> attributes = ProtobufMessageConverter.convertToAttributes(message, fieldToAttributeMapping,
                e -> componentLogger.warn("Unable to extract field from message: " + e.getMessage(), e));
        flowFile = session.putAllAttributes(flowFile, attributes);

        return flowFile;
    }
}
