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
package org.apache.nifi.processors.grpc.service;

import com.google.protobuf.Message;
import io.grpc.BindableService;
import io.grpc.MethodDescriptor;
import io.grpc.ServiceDescriptor;
import io.grpc.stub.StreamObserver;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.nifi.processors.grpc.compiler.ProtoDirectories;
import org.apache.nifi.processors.grpc.compiler.ProtoUtils;
import org.apache.nifi.util.file.classloader.ClassLoaderUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CustomIngestServiceGenerator {

    private static final Logger LOGGER = LoggerFactory.getLogger(CustomIngestServiceGenerator.class);

    private final CustomIngestServiceHandler serviceHandler;

    public CustomIngestServiceGenerator(CustomIngestServiceHandler serviceHandler) {
        this.serviceHandler = serviceHandler;
    }

    public Set<BindableService> generate(ProtoDirectories protoDirectories) {
        Set<BindableService> services = new HashSet<>();

        ClassLoader classLoader = createClassLoader(protoDirectories);

        List<String> serviceClassNames = ProtoUtils.getServiceClassNames(protoDirectories);

        for (String serviceClassName: serviceClassNames) {
            ServiceDescriptor serviceDescriptor = getServiceDescriptor(serviceClassName, classLoader);

            Class<?> serviceImplBaseClass = getServiceImplBaseClass(serviceDescriptor, serviceClassName, classLoader);

            Map<String, MethodDescriptor<?, ?>> serviceMethods = getServiceMethods(serviceDescriptor);

            BindableService serviceImpl = createServiceImpl(serviceImplBaseClass, serviceMethods);
            services.add(serviceImpl);
        }

        return services;
    }

    private ClassLoader createClassLoader(ProtoDirectories protoDirectories) {
        String classpath = protoDirectories.getMessagesDir() + "," + protoDirectories.getServicesDir();
        try {
            return ClassLoaderUtils.getCustomClassLoader(classpath, getClass().getClassLoader(), null);
        } catch (Exception e) {
            throw new CustomIngestServiceException("Unable to create custom ClassLoader for classpath: " + classpath, e);
        }
    }

    private ServiceDescriptor getServiceDescriptor(String serviceClassName, ClassLoader classLoader) {
        try {
            Class<?> serviceClass = classLoader.loadClass(serviceClassName);

            return (ServiceDescriptor) MethodUtils.invokeStaticMethod(serviceClass, "getServiceDescriptor");
        } catch (Exception e) {
            throw new CustomIngestServiceException("Unable to get protobuf service descriptor for service: " + serviceClassName, e);
        }
    }

    private Class<?> getServiceImplBaseClass(ServiceDescriptor serviceDescriptor, String serviceClassName, ClassLoader classLoader) {
        try {
            String serviceSimpleName = StringUtils.substringAfterLast(serviceDescriptor.getName(), '.');
            String serviceImplBaseClassName = String.format("%s$%sImplBase", serviceClassName, serviceSimpleName);
            return classLoader.loadClass(serviceImplBaseClassName);
        } catch (Exception e) {
            throw new CustomIngestServiceException("Unable to load service implementation base class for service: " + serviceClassName, e);
        }
    }

    private Map<String, MethodDescriptor<?, ?>> getServiceMethods(ServiceDescriptor serviceDescriptor) {
        Map<String, MethodDescriptor<?, ?>> serviceMethods = new HashMap<>();

        for (MethodDescriptor<?, ?> methodDescriptor : serviceDescriptor.getMethods()) {
            serviceMethods.put(StringUtils.uncapitalize(methodDescriptor.getBareMethodName()), methodDescriptor);
        }

        return serviceMethods;
    }

    private BindableService createServiceImpl(Class<?> serviceImplBaseClass, Map<String, MethodDescriptor<?, ?>> serviceMethods) {
        Enhancer enhancer = new Enhancer();
        enhancer.setSuperclass(serviceImplBaseClass);

        enhancer.setCallback((MethodInterceptor) (obj, method, args, proxy) -> {
            MethodDescriptor<?, ?> methodDescriptor = serviceMethods.get(method.getName());
            if (methodDescriptor != null) {
                switch (methodDescriptor.getType()) {
                    case UNARY:
                        serviceHandler.handleUnaryCall(methodDescriptor, (Message) args[0], (StreamObserver<Message>) args[1]);
                        return null;
                    case SERVER_STREAMING:
                        serviceHandler.handleServerStreamingCall(methodDescriptor, (Message) args[0], (StreamObserver<Message>) args[1]);
                        return null;
                    case CLIENT_STREAMING:
                        return serviceHandler.handleClientStreamingCall(methodDescriptor, (StreamObserver<Message>) args[0]);
                    case BIDI_STREAMING:
                        return serviceHandler.handleBidirectionalStreamingCall(methodDescriptor, (StreamObserver<Message>) args[0]);
                    default:
                        throw new UnsupportedOperationException("Unsupported GRPC service method type: " + methodDescriptor.getType());
                }
            } else {
                return proxy.invokeSuper(obj, args);
            }
        });

        return (BindableService) enhancer.create();
    }
}
