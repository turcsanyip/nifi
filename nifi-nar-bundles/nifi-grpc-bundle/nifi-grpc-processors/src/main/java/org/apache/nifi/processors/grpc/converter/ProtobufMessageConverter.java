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
package org.apache.nifi.processors.grpc.converter;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import org.apache.commons.lang3.StringUtils;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class ProtobufMessageConverter {

    public static byte[] convertToPayload(Message message, String fieldPath) {
        byte[] payload;
        if (fieldPath == null) {
            payload = serialize(message);
        } else if (StringUtils.isNotBlank(fieldPath)) {
            Object fieldValue = ProtobufFieldPathEvaluator.evaluatePath(message, fieldPath);
            payload = serialize(fieldValue);
        } else {
            payload = null;
        }

        return payload;
    }

    private static byte[] serialize(Object fieldValue) {
        if (fieldValue == null) {
            return null;
        } else if (fieldValue instanceof Message) {
            Message message = (Message) fieldValue;
            String json = convertToJson(message);
            return json.getBytes(StandardCharsets.UTF_8);
        } else if (fieldValue instanceof ByteString) {
            ByteString byteString = (ByteString) fieldValue;
            return byteString.toByteArray();
        } else if (fieldValue instanceof List) {
            throw new ProtobufConverterException("Repeated fields are not supported");
        } else {
            return fieldValue.toString().getBytes(StandardCharsets.UTF_8);
        }
    }

    private static String convertToJson(Message message) {
        try {
            JsonFormat.Printer jsonPrinter = JsonFormat.printer();
            String json = jsonPrinter.print(message);
            return json;
        } catch (InvalidProtocolBufferException e) {
            throw new ProtobufConverterException("Unable to convert Protocol Buffers message to JSON");
        }
    }

    public static Map<String, String> convertToAttributes(Message message, Map<String, String> fieldPaths, Consumer<Exception> errorHandler) {
        Map<String, String> attributes = new HashMap<>();

        for (Map.Entry<String, String> entry: fieldPaths.entrySet()) {
            String fieldPath = entry.getKey();
            String attributeName = entry.getValue();

            try {
                Object fieldValue = ProtobufFieldPathEvaluator.evaluatePath(message, fieldPath);

                attributes.put(attributeName, String.valueOf(fieldValue));
            } catch (Exception e) {
                errorHandler.accept(e);
            }
        }

        return attributes;
    }
}
