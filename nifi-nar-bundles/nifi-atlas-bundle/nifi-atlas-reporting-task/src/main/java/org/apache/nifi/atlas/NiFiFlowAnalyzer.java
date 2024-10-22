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
package org.apache.nifi.atlas;

import org.apache.nifi.atlas.model.NiFiAtlasEntity;
import org.apache.nifi.atlas.model.NiFiFlow;
import org.apache.nifi.atlas.model.NiFiFlowPath;
import org.apache.nifi.atlas.model.NiFiQueue;
import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.controller.status.PortStatus;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.nifi.atlas.model.NiFiFlowPath.createDeepLinkUrl;

public class NiFiFlowAnalyzer {

    private static final Logger logger = LoggerFactory.getLogger(NiFiFlowAnalyzer.class);

    private final Set<String> traversedPathIds = new HashSet<>();

    private NiFiFlowAnalyzer() {
    }

    public static void analyze(NiFiFlow nifiFlow, ProcessGroupStatus rootProcessGroup) {
        final NiFiFlowAnalyzer analyzer = new NiFiFlowAnalyzer();

        analyzer.analyzeProcessGroup(nifiFlow, rootProcessGroup);
        analyzer.analyzePaths(nifiFlow);
    }

    void analyzeProcessGroup(NiFiFlow nifiFlow, ProcessGroupStatus processGroupStatus) {
        processGroupStatus.getProcessorStatus().forEach(p -> nifiFlow.addProcessor(p));
        processGroupStatus.getConnectionStatus().forEach(c -> nifiFlow.addConnection(c));

        // PortStatus does not have isRemotePort() method but it can be determined via isTransmitting() because it is only set for remote ports
        processGroupStatus.getInputPortStatus().stream()
                .filter(port -> port.isTransmitting() != null)
                .forEach(port -> nifiFlow.addRemoteInputPort(port));
        processGroupStatus.getOutputPortStatus().stream()
                .filter(port -> port.isTransmitting() != null)
                .forEach(port -> nifiFlow.addRemoteOutputPort(port));

        // Analyze child ProcessGroups recursively.
        for (ProcessGroupStatus child : processGroupStatus.getProcessGroupStatus()) {
            analyzeProcessGroup(nifiFlow, child);
        }
    }

    private List<String> getIncomingProcessComponents(NiFiFlow nifiFlow, String componentId) {
        final List<ConnectionStatus> ins = nifiFlow.getIncomingConnections(componentId);
        if (ins == null) {
            return Collections.emptyList();
        }

        final List<String> ids = new ArrayList<>();
        for (ConnectionStatus in : ins) {
            final String sourceId = in.getSourceId();
            if (sourceId.equals(in.getDestinationId())) {
                // Ignore self relationship.
                continue;
            }

            if (nifiFlow.isProcessComponent(sourceId)) {
                ids.add(sourceId);
            } else {
                ids.addAll(getIncomingProcessComponents(nifiFlow, sourceId));
            }
        }

        return ids;
    }

    private List<String> getOutgoingProcessComponents(NiFiFlow nifiFlow, String componentId) {
        final List<ConnectionStatus> outs = nifiFlow.getOutgoingConnections(componentId);
        if (outs == null || outs.isEmpty()) {
            return Collections.emptyList();
        }

        final List<String> ids = new ArrayList<>();
        for (ConnectionStatus out : outs) {
            final String destinationId = out.getDestinationId();
            if (destinationId.equals(out.getSourceId())) {
                // Ignore self relationship.
                continue;
            }

            if (nifiFlow.isProcessComponent(destinationId)) {
                ids.add(destinationId);
            } else {
                ids.addAll(getOutgoingProcessComponents(nifiFlow, destinationId));
            }
        }
        return ids;
    }

    private void traverse(NiFiFlow nifiFlow, NiFiFlowPath path, String processComponentId) {
        addTraversedPath(path.getId());

        // Add the current process component (processor or remote input/output port) to the flow path
        path.addProcessComponent(processComponentId);

        // Analyze the destination process components (if any).
        final List<String> outgoingProcessComponents = getOutgoingProcessComponents(nifiFlow, processComponentId);
        outgoingProcessComponents.forEach(destinationId -> {
            if (path.getProcessComponents().contains(destinationId)) {
                // Avoid looping back to the current path.
                return;
            }

            // If there are multiple destinations or the destination has more than one inputs,
            // then it should be treated as a separate flow path.
            final boolean createJointPoint = outgoingProcessComponents.size() > 1
                    || getIncomingProcessComponents(nifiFlow, destinationId).size() > 1;

            if (createJointPoint) {

                // Get or create a queue DataSet as a join point to the destination flow path.
                // This queue is used for linking flow path Process entities together on Atlas lineage graph.
                final NiFiQueue queue = nifiFlow.getOrCreateQueue(destinationId);
                path.connectOutputQueue(queue);

                // If the destination has already been traversed, it must not be visited again.
                final boolean alreadyTraversed = isTraversedPath(destinationId);
                if (alreadyTraversed) {
                    return;
                }

                // Create and initialize a new flow path for the destination (as it is not traversed yet).
                final NiFiFlowPath destinationPath = nifiFlow.getOrCreateFlowPath(destinationId);
                destinationPath.connectInputQueue(queue);

                // Start traversing the destination as a separate flow path.
                traverse(nifiFlow, destinationPath, destinationId);

            } else {
                // Normal relation, continue digging.
                traverse(nifiFlow, path, destinationId);
            }

        });
    }

    private boolean isHeadProcessor(NiFiFlow nifiFlow, String componentId) {
        final List<ConnectionStatus> ins = nifiFlow.getIncomingConnections(componentId);
        if (ins == null || ins.isEmpty()) {
            return true;
        }
        return ins.stream().allMatch(
                in -> {
                    // If it has incoming relationship from other process components, then return false.
                    final String sourceId = in.getSourceId();
                    if (nifiFlow.isProcessComponent(sourceId)) {
                        return false;
                    }
                    // Check next level.
                    return isHeadProcessor(nifiFlow, sourceId);
                }
        );
    }

    private void addTraversedPath(String pathId) {
        traversedPathIds.add(pathId);
    }

    private boolean isTraversedPath(String pathId) {
        return traversedPathIds.contains(pathId);
    }

    void analyzePaths(NiFiFlow nifiFlow) {
        // Now let's break it into flow paths.
        final Map<String, ProcessorStatus> processors = nifiFlow.getProcessors();
        final Set<String> headProcessComponents = processors.keySet().stream()
                .filter(pid -> isHeadProcessor(nifiFlow, pid))
                .collect(Collectors.toSet());

        // Use RemoteInputPorts as head components.
        final Map<String, PortStatus> remoteInputPorts = nifiFlow.getRemoteInputPorts();
        headProcessComponents.addAll(remoteInputPorts.keySet());

        headProcessComponents.forEach(headComponentId -> {
            // By using the headComponentId as its qualifiedName, it's guaranteed that
            // the same path will end up being the same Atlas entity.
            // However, if the first processor is replaced by another,
            // the flow path will have a different id, and the old path is logically deleted.
            final NiFiFlowPath path = nifiFlow.getOrCreateFlowPath(headComponentId); //
            traverse(nifiFlow, path, headComponentId);
        });

        final Map<String, PortStatus> remoteOutputPorts = nifiFlow.getRemoteOutputPorts();

        // set flow path attributes: name, description, url
        nifiFlow.getFlowPaths().values().stream()
                .filter(NiFiAtlasEntity::isActive)
                .forEach(path -> {
                    final StringBuilder name = new StringBuilder();
                    final StringBuilder description = new StringBuilder();
                    path.getProcessComponents().forEach(componentId -> {
                        final String componentName = nifiFlow.getProcessComponentName(componentId);

                        if (name.length() > 0) {
                            name.append(", ");
                            description.append(", ");
                        }
                        name.append(componentName);
                        description.append(String.format("%s::%s", componentName, componentId));
                    });

                    path.setName(name.toString());
                    path.setDescription(description.toString());

                    final String pathId = path.getId();
                    final String groupId;
                    if (processors.containsKey(pathId)) {
                        final ProcessorStatus processor = processors.get(pathId);
                        groupId = processor.getGroupId();
                    } else if (remoteInputPorts.containsKey(pathId)) {
                        final PortStatus port = remoteInputPorts.get(pathId);
                        groupId = port.getGroupId();
                    } else if (remoteOutputPorts.containsKey(pathId)) {
                        final PortStatus port = remoteOutputPorts.get(pathId);
                        groupId = port.getGroupId();
                    } else {
                        logger.warn("Head component not found for FlowPath ID: {}", pathId);
                        groupId = nifiFlow.getId();
                    }

                    path.setUrl(createDeepLinkUrl(nifiFlow.getUrl(), groupId, pathId));
                });
    }
}