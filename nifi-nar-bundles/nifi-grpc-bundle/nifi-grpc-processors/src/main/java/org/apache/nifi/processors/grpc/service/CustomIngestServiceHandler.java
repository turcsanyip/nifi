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
import io.grpc.MethodDescriptor;
import io.grpc.stub.StreamObserver;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processors.grpc.ListenGRPC;
import org.apache.nifi.processors.grpc.util.BackpressureChecker;
import org.apache.nifi.processors.grpc.util.ProtobufProcessorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class CustomIngestServiceHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(CustomIngestServiceHandler.class);

    private static final int SESSION_FACTORY_RETRY_TIMEOUT_MS = 60_000;
    private static final int SESSION_FACTORY_RETRY_INTERVAL_MS = 100;
    private static final int SESSION_FACTORY_RETRY_COUNT = SESSION_FACTORY_RETRY_TIMEOUT_MS / SESSION_FACTORY_RETRY_INTERVAL_MS;

    private final AtomicReference<ProcessSessionFactory> sessionFactoryReference;
    private final ComponentLog componentLogger;
    private final String contentField;
    private final Map<String, String> fieldToAttributeMapping;
    private final BackpressureChecker backpressureChecker;

    public CustomIngestServiceHandler(AtomicReference<ProcessSessionFactory> sessionFactoryReference, ComponentLog componentLogger, String contentField,
                                      Map<String, String> fieldToAttributeMapping, BackpressureChecker backpressureChecker) {
        this.sessionFactoryReference = sessionFactoryReference;
        this.componentLogger = componentLogger;
        this.contentField = contentField;
        this.fieldToAttributeMapping = fieldToAttributeMapping;
        this.backpressureChecker = backpressureChecker;
    }

    public void handleUnaryCall(MethodDescriptor<?, ?> methodDescriptor, Message request, StreamObserver<Message> responseObserver) {
        try {
            processRequest(methodDescriptor, request);

            responseObserver.onNext(null);
            responseObserver.onCompleted();
        } catch (Exception e) {
            componentLogger.error("Error occurred during processing request", e);
            responseObserver.onError(e);
        }
    }

    public void handleServerStreamingCall(MethodDescriptor<?, ?> methodDescriptor, Message request, StreamObserver<Message> responseObserver) {
        handleUnaryCall(methodDescriptor, request, responseObserver);
    }

    public StreamObserver<? extends Message> handleClientStreamingCall(MethodDescriptor<?, ?> methodDescriptor, StreamObserver<? extends Message> responseObserver) {
        return new StreamObserver<Message>() {
            @Override
            public void onNext(Message request) {
                try {
                    processRequest(methodDescriptor, request);
                } catch (Exception e) {
                    componentLogger.error("Error occurred during processing request", e);
                    responseObserver.onError(e);
                }
            }

            @Override
            public void onError(Throwable thrwbl) {
                componentLogger.error("Client streaming error occurred", thrwbl);
            }

            @Override
            public void onCompleted() {
                responseObserver.onNext(null);
                responseObserver.onCompleted();
            }
        };
    }

    public StreamObserver<? extends Message> handleBidirectionalStreamingCall(MethodDescriptor<?, ?> methodDescriptor, StreamObserver<? extends Message> responseObserver) {
        return handleClientStreamingCall(methodDescriptor, responseObserver);
    }

    private void processRequest(MethodDescriptor<?, ?> methodDescriptor, Message request) {
        if (backpressureChecker.isBackpressure()) {
            throw new CustomIngestServiceException("Backpressure occurred, cannot process request.");
        }

        ProcessSession session = null;

        try {
            session = createSession();
            FlowFile flowFile = session.create();

            flowFile = ProtobufProcessorUtils.convertMessageToFlowFile(session, flowFile, componentLogger, request, contentField, fieldToAttributeMapping);

            String transitUri = String.format("grpc://%s", methodDescriptor.getFullMethodName());

            session.getProvenanceReporter().receive(flowFile, transitUri);
            session.transfer(flowFile, ListenGRPC.REL_SUCCESS);
            session.commit();
        } catch (Exception e) {
            if (session != null) {
                session.rollback();
            }
            throw e;
        }
    }

    private ProcessSession createSession() {
        ProcessSessionFactory sessionFactory = sessionFactoryReference.get();

        int retries = 0;
        while (sessionFactory == null) {
            if (retries++ > SESSION_FACTORY_RETRY_COUNT) {
                throw new CustomIngestServiceException(String.format("Unable to create ProcessSession. SessionFactoryReference was not initialized within %d ms", SESSION_FACTORY_RETRY_TIMEOUT_MS));
            }

            try {
                Thread.sleep(SESSION_FACTORY_RETRY_INTERVAL_MS);
            } catch (final InterruptedException e) {
            }

            sessionFactory = sessionFactoryReference.get();
        }

        return sessionFactory.createSession();
    }
}
