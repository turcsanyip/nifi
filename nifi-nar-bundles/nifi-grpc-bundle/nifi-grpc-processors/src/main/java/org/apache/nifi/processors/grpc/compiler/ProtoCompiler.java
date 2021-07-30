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

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ProtoCompiler {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProtoCompiler.class);

    private enum OsType {
        LINUX("linux"),
        MAC_OSX("osx"),
        WINDOWS("windows");

        private final String tag;

        OsType(String tag) {
            this.tag = tag;
        }

        String getTag() {
            return tag;
        }
    }

    private static final String OS_ARCH_X86_64 = "x86_64";

    private static final String PROTOC_EXE_TEMPLATE = "protoc-%s-%s-x86_64.exe";
    private static final String PROTOC_GRPC_EXE_TEMPLATE = "protoc-gen-grpc-java-%s-%s-x86_64.exe";

    private final Path protocExe;
    private final Path protocGrpcExe;

    public static ProtoDirectories compile(String protoDirectory, String javaDirectoryPrefix) {
        Path protoDir = Paths.get(protoDirectory);
        Path javaDir = createTempDir(javaDirectoryPrefix);
        ProtoDirectories protoDirectories = new ProtoDirectories(protoDir, javaDir);

        ProtoCompiler protoCompiler = new ProtoCompiler();
        protoCompiler.compile(protoDirectories); // message only would be enough in case of protobuf

        return protoDirectories;
    }

    private static Path createTempDir(String prefix) {
        try {
            return Files.createTempDirectory(prefix);
        } catch (IOException e) {
            throw new ProtoException("Couldn't create temp directory with prefix: " + prefix);
        }
    }

    public ProtoCompiler() {
        OsType osType;
        if (SystemUtils.IS_OS_LINUX) {
            osType = OsType.LINUX;
        } else if (SystemUtils.IS_OS_MAC_OSX) {
            osType = OsType.MAC_OSX;
        } else if (SystemUtils.IS_OS_WINDOWS) {
            osType = OsType.WINDOWS;
        } else {
            throw new ProtoException("Only Linux, Mac OSX and Windows are supported");
        }

        if (!OS_ARCH_X86_64.equals(System.getProperty("os.arch"))) {
            throw new ProtoException("Only x86-64 architectures are supported");
        }

        ProtoProperties protoProperties = new ProtoProperties();
        String protobufVersion = protoProperties.getProtobufVersion();
        String grpcVersion = protoProperties.getGrpcVersion();

        Path baseDir = Paths.get(getJarLocation(ProtoCompiler.class)).getParent();

        protocExe = baseDir.resolve(String.format(PROTOC_EXE_TEMPLATE, protobufVersion, osType.getTag()));
        protocGrpcExe = baseDir.resolve(String.format(PROTOC_GRPC_EXE_TEMPLATE, grpcVersion, osType.getTag()));

        makeExecutable(protocExe, osType);
        makeExecutable(protocGrpcExe, osType);
    }

    private void makeExecutable(Path path, OsType osType) {
        if (osType == OsType.LINUX || osType == OsType.MAC_OSX) { // TODO: Windows?
            if (!Files.isExecutable(path)) {
                if (!path.toFile().setExecutable(true)) {
                    throw new ProtoException(String.format("%s cannot be made executable", path));
                }
            }
        }
    }

    public void compile(ProtoDirectories protoDirectories) {
        checkJavaDir(protoDirectories.getJavaDir());

        createJavaSubDirs(protoDirectories);

        compileProto(protoDirectories);
        compileJava(protoDirectories.getJavaDir());
    }

    private void checkJavaDir(Path javaDir) {
        try {
            if (!Files.isDirectory(javaDir) || !Files.exists(javaDir)) {
                throw new ProtoException("Target directory does not exists or not a directory: " + javaDir);
            }
            if (Files.list(javaDir).findAny().isPresent()) {
                throw new ProtoException("Target directory is not empty: " + javaDir);
            }
        } catch (IOException e) {
            throw new ProtoException("Unable to compile proto files", e);
        }
    }

    private void createJavaSubDirs(ProtoDirectories protoDirectories) {
        try {
            Files.createDirectory(protoDirectories.getMessagesDir());
            Files.createDirectory(protoDirectories.getServicesDir());
        } catch (IOException e) {
            throw new ProtoException("Unable to compile proto files", e);
        }
    }

    private void compileProto(ProtoDirectories protoDirectories) {
        List<Path> protoFiles = ProtoUtils.listFiles(protoDirectories.getProtoDir(), ProtoUtils.PROTO_FILE_EXT);
        for (Path protoFile: protoFiles) {
            compileProto(protoFile, protoDirectories);
        }
    }

    private void compileProto(Path protoFile, ProtoDirectories protoDirectories) {
        try {
//            String command =  protocExe +
//                    " --plugin=" + protocGrpcExe +
//                    " --java_out=" + javaDir.resolve("messages") +
//                    " --grpc-java_out=" + javaDir.resolve("services") +
//                    " --proto_path=" + protoDir +
//                    " " + protoFile;
//
//            LOGGER.debug("Running proto compile command: " + command);
//
//            Runtime.getRuntime().exec(command);

            Process process = new ProcessBuilder()
                    .command(protocExe.toString(),
                            "--plugin=protoc-gen-grpc-java=" + protocGrpcExe,
                            "--java_out=" + protoDirectories.getMessagesDir(),
                            "--grpc-java_out=" + protoDirectories.getServicesDir(),
                            "--proto_path=" + protoDirectories.getProtoDir(),
                            protoFile.toString())
                    .start();

            boolean finished = process.waitFor(10000, TimeUnit.MILLISECONDS);
            int exitValue = process.exitValue();

            if (finished && exitValue == 0) {
                LOGGER.info("Proto file compiled: {}", protoFile);
            } else {
                String errorMessage = new String(IOUtils.toByteArray(process.getErrorStream()));
                throw new ProtoException(String.format("Failed to compile proto file %s%n%s", protoFile, errorMessage));
            }
        } catch (IOException | InterruptedException e) {
            throw new ProtoException("Proto compilation error (" + protoFile + ")", e);
        }
    }

    private void compileJava(Path javaDir) {
        List<File> javaFiles = ProtoUtils.listFiles(javaDir, ProtoUtils.JAVA_FILE_EXT)
                .stream()
                .map(Path::toFile)
                .collect(Collectors.toList());

        JavaCompiler javaCompiler = ToolProvider.getSystemJavaCompiler();
        DiagnosticCollector<JavaFileObject> diagnosticCollector = new DiagnosticCollector<>();
        StandardJavaFileManager fileManager = javaCompiler.getStandardFileManager(diagnosticCollector, null, null);

        List<String> options = Arrays.asList("-classpath",
                String.join(File.pathSeparator,
                        getJarLocation(com.google.protobuf.Message.class),
                        getJarLocation(io.grpc.BindableService.class),
                        getJarLocation(io.grpc.stub.StreamObserver.class),
                        getJarLocation(io.grpc.protobuf.ProtoUtils.class),
                        getJarLocation(com.google.common.util.concurrent.ListenableFuture.class)
                )
        );

        Iterable<? extends JavaFileObject> units = fileManager.getJavaFileObjectsFromFiles(javaFiles);

        JavaCompiler.CompilationTask task = javaCompiler.getTask(null, fileManager, diagnosticCollector, options, null, units);

        Boolean success = task.call();
        if (!success) {
            for (Diagnostic<? extends JavaFileObject> diagnostic : diagnosticCollector.getDiagnostics()) {
                LOGGER.error("Java compilation error: " + diagnostic.toString());
            }
            throw new ProtoException("Couldn't compile java files.");
        }
    }

    private String getJarLocation(Class<?> clazz) {
        return clazz.getProtectionDomain().getCodeSource().getLocation().getFile();
    }
}
