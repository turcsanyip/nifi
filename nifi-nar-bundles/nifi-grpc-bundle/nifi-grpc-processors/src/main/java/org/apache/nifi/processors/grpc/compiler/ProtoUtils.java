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

import org.apache.commons.lang3.StringUtils;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ProtoUtils {

    public static final String PROTO_FILE_EXT = ".proto";
    public static final String JAVA_FILE_EXT = ".java";
    public static final String CLASS_FILE_EXT = ".class";

    public static List<Path> listFiles(Path directory, String extension) {
        try {
            return Files.walk(directory)
                    .filter(Files::isRegularFile)
                    .filter(filePath -> filePath.toString().endsWith(extension))
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new ProtoException("Couldn't access directory: " + directory);
        }
    }

    public static List<String> getMessageClassNames(ProtoDirectories protoDirectories) {
        return getClassNames(protoDirectories.getMessagesDir(), ProtoUtils.CLASS_FILE_EXT);
    }

    public static List<String> getServiceClassNames(ProtoDirectories protoDirectories) {
        return getClassNames(protoDirectories.getServicesDir(), ProtoUtils.JAVA_FILE_EXT);
    }

    private static List<String> getClassNames(Path directory, String extension) {
        List<String> classNames = new ArrayList<>();

        List<Path> files = listFiles(directory, extension);

        for (Path file : files) {
            Path relativePath = directory.relativize(file);
            String className = StringUtils.substringBeforeLast(StringUtils.replaceChars(relativePath.toString(), '/', '.'), extension);
            classNames.add(className);
        }

        return classNames;
    }
}
