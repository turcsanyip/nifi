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

import java.nio.file.Path;

public class ProtoDirectories {

    private static final String MESSAGES_SUBDIR = "messages";
    private static final String SERVICES_SUBDIR = "services";

    private final Path protoDir;
    private final Path javaDir;

    private final Path messagesDir;
    private final Path servicesDir;

    ProtoDirectories(Path protoDir, Path javaDir) {
        this.protoDir = protoDir;
        this.javaDir = javaDir;

        messagesDir = javaDir.resolve(MESSAGES_SUBDIR);
        servicesDir = javaDir.resolve(SERVICES_SUBDIR);
    }

    public Path getProtoDir() {
        return protoDir;
    }

    public Path getJavaDir() {
        return javaDir;
    }

    public Path getMessagesDir() {
        return messagesDir;
    }

    public Path getServicesDir() {
        return servicesDir;
    }
}
