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

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.Queue;

import static com.google.protobuf.Descriptors.FieldDescriptor.Type.MESSAGE;

class ProtobufFieldPathEvaluator {

    static Object evaluatePath(Message message, String fieldPath) {
        return evaluatePath(message, fieldPath, new LinkedList<>(Arrays.asList(fieldPath.split("\\."))));
    }

    private static Object evaluatePath(Message message, String fieldPath, Queue<String> fieldsQueue)  {
        if (fieldsQueue.isEmpty()) {
            throw new IllegalArgumentException(String.format("Fields queue is empty (field path: %s)",  fieldPath));
        }

        String fieldName = fieldsQueue.remove();

        Descriptors.FieldDescriptor fieldDescriptor = message.getDescriptorForType().findFieldByName(fieldName);

        if (fieldDescriptor == null) {
            throw new ProtobufConverterException(String.format("Field '%s' not found (field path: %s)", fieldName,  fieldPath));
        }

        Object fieldValue = message.getField(fieldDescriptor);

        if (fieldsQueue.isEmpty()) {
            return fieldValue;
        } else {
            if (fieldDescriptor.getType() != MESSAGE) {
                throw new ProtobufConverterException(String.format("Field '%s' is not type of Message (field path: %s)", fieldName, fieldPath));
            }

            if (fieldDescriptor.isRepeated()) {
                throw new ProtobufConverterException(String.format("Repeated/map field '%s' is not supported (field path: %s)", fieldName, fieldPath));
            }

            return evaluatePath((Message) fieldValue, fieldPath, fieldsQueue);
        }
    }
}
