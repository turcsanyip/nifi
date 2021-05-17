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
package org.apache.nifi.processors.grpc.compiler;

import java.io.IOException;
import java.util.Properties;

class ProtoProperties {

    private static final String PROPERTIES_LOCATION = "proto.properties";

    private static final String PROTOBUF_VERSION_KEY = "protobuf.version";
    private static final String GRPC_VERSION_KEY = "grpc.version";

    private final Properties properties;

    ProtoProperties() {
        properties = new Properties();
        try {
            properties.load(ProtoProperties.class.getClassLoader().getResourceAsStream(PROPERTIES_LOCATION));
        } catch (IOException e) {
            throw new ProtoException("Unable to load protoc executables versions", e);
        }
    }

    String getProtobufVersion() {
        return properties.getProperty(PROTOBUF_VERSION_KEY);
    }

    String getGrpcVersion() {
        return properties.getProperty(GRPC_VERSION_KEY);
    }
}
