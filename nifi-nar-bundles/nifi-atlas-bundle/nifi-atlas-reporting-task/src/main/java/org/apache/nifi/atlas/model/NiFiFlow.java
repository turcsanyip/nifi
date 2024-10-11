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
package org.apache.nifi.atlas.model;

import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasRelationship;
import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.controller.status.PortStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.nifi.atlas.AtlasUtils.getComponentIdFromQualifiedName;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_ENTITY_STATUS;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_FLOW_PATHS;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_INPUT_PORTS;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_OUTPUT_PORTS;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUEUES;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_RELATIONSHIP_GUID;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_RELATIONSHIP_STATUS;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_TYPE_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_URL;
import static org.apache.nifi.atlas.NiFiTypes.REL_ATTR_INPUT_TO_PROCESSES;
import static org.apache.nifi.atlas.NiFiTypes.REL_ATTR_OUTPUT_FROM_PROCESSES;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_FLOW;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_FLOW_PATH;

public class NiFiFlow extends AbstractNiFiAtlasEntity {

    private static final Logger logger = LoggerFactory.getLogger(NiFiFlow.class);

    private final String namespace;

    // Map[NiFi component ID -> NiFi component object]
    private final Map<String, ProcessorStatus> processors = new HashMap<>();
    private final Map<String, List<ConnectionStatus>> incomingConnections = new HashMap<>();
    private final Map<String, List<ConnectionStatus>> outgoingConnections = new HashMap<>();
    private final Map<String, PortStatus> remoteInputPorts = new HashMap<>();
    private final Map<String, PortStatus> remoteOutputPorts = new HashMap<>();

    // Map[NiFi component ID -> NiFi Atlas entity wrapper]
    private final Map<String, NiFiFlowPath> flowPaths = new HashMap<>();
    private final Map<String, NiFiQueue> queues = new HashMap<>();
    private final Map<String, NiFiInputPort> inputPorts = new HashMap<>();
    private final Map<String, NiFiOutputPort> outputPorts = new HashMap<>();


    public NiFiFlow(String rootProcessGroupId, String namespace) {
        super(TYPE_NIFI_FLOW, rootProcessGroupId, namespace);

        this.namespace = namespace;
    }

    public NiFiFlow(AtlasEntity nifiFlowEntity, String namespace) {
        super(nifiFlowEntity);

        this.namespace = namespace;

        removeAttribute(ATTR_FLOW_PATHS);
        removeAttribute(ATTR_QUEUES);
        removeAttribute(ATTR_INPUT_PORTS);
        removeAttribute(ATTR_OUTPUT_PORTS);

        notifyActive();
    }

    public String getNamespace() {
        return namespace;
    }

    public String getUrl() {
        return getStringAttribute(ATTR_URL);
    }

    public void setUrl(String url) {
        setStringAttribute(ATTR_URL, url);
    }

    public void addProcessor(ProcessorStatus p) {
        processors.put(p.getId(), p);
    }

    public Map<String, ProcessorStatus> getProcessors() {
        return processors;
    }

    public List<ConnectionStatus> getIncomingConnections(String componentId) {
        return incomingConnections.get(componentId);
    }

    public void addConnection(ConnectionStatus c) {
        outgoingConnections.computeIfAbsent(c.getSourceId(), k -> new ArrayList<>()).add(c);
        incomingConnections.computeIfAbsent(c.getDestinationId(), k -> new ArrayList<>()).add(c);
    }

    public List<ConnectionStatus> getOutgoingConnections(String componentId) {
        return outgoingConnections.get(componentId);
    }

    public void addRemoteInputPort(PortStatus port) {
        remoteInputPorts.put(port.getId(), port);
        createOrUpdateInputPort(port.getId(), port.getName());
    }

    public Map<String, PortStatus> getRemoteInputPorts() {
        return remoteInputPorts;
    }

    public void addRemoteOutputPort(PortStatus port) {
        remoteOutputPorts.put(port.getId(), port);
        createOrUpdateOutputPort(port.getId(), port.getName());
    }

    public Map<String, PortStatus> getRemoteOutputPorts() {
        return remoteOutputPorts;
    }

    public void addFlowPath(NiFiFlowPath flowPath) {
        flowPaths.put(flowPath.getId(), flowPath);
    }

    public void addQueue(NiFiQueue queue) {
        queues.put(queue.getId(), queue);
    }

    public void addInputPort(NiFiInputPort inputPort) {
        inputPorts.put(inputPort.getId(), inputPort);
    }

    public void addOutputPort(NiFiOutputPort outputPort) {
        outputPorts.put(outputPort.getId(), outputPort);
    }

    public void connectFlowPathsAndQueues() {
        for (NiFiQueue queue : queues.values()) {
            final List<Map<String, String>> inputToProcesses = queue.getRelationshipAttribute(REL_ATTR_INPUT_TO_PROCESSES);
            connectFlowPathsAndQueues(inputToProcesses, (pathId, relationshipGuid) -> flowPaths.get(pathId).addInputQueue(queue, relationshipGuid));

            final List<Map<String, String>> outputFromProcesses = queue.getRelationshipAttribute(REL_ATTR_OUTPUT_FROM_PROCESSES);
            connectFlowPathsAndQueues(outputFromProcesses, (pathId, relationshipGuid) -> flowPaths.get(pathId).addOutputQueue(queue, relationshipGuid));

            queue.removeRelationshipAttributes();
        }
    }

    private void connectFlowPathsAndQueues(List<Map<String, String>> relationships, BiConsumer<String, String> consumer) {
        for (Map<String, String> relationship : relationships) {
            final String entityType = relationship.get(ATTR_TYPE_NAME);
            final String entityStatus = relationship.get(ATTR_ENTITY_STATUS);
            final String relationshipStatus = relationship.get(ATTR_RELATIONSHIP_STATUS);
            if (TYPE_NIFI_FLOW_PATH.equals(entityType)
                    && AtlasEntity.Status.ACTIVE.name().equals(entityStatus)
                    && AtlasRelationship.Status.ACTIVE.name().equals(relationshipStatus)) {
                final String pathId = getComponentIdFromQualifiedName(relationship.get(ATTR_QUALIFIED_NAME));
                final String relationshipGuid = relationship.get(ATTR_RELATIONSHIP_GUID);
                consumer.accept(pathId, relationshipGuid);
            }
        }
    }

    public void updateFlowComponentReferences() {
        updateFlowComponentReferences(flowPaths, ATTR_FLOW_PATHS);
        updateFlowComponentReferences(queues, ATTR_QUEUES);
        updateFlowComponentReferences(inputPorts, ATTR_INPUT_PORTS);
        updateFlowComponentReferences(outputPorts, ATTR_OUTPUT_PORTS);
    }

    private void updateFlowComponentReferences(final Map<String, ? extends NiFiComponent> flowComponents, final String attributeName) {
        final boolean changed = flowComponents.values().stream()
                .anyMatch(fc -> fc.isCreated() || !fc.isActive());

        if (changed) {
            final List<AtlasObjectId> objectIds = flowComponents.values().stream()
                    .filter(NiFiAtlasEntity::isActive)
                    .map(fc -> new AtlasObjectId(fc.getGuid()))
                    .collect(Collectors.toList());

            setAttribute(attributeName, objectIds);
        }
    }

    public NiFiFlowPath getOrCreateFlowPath(String pathId) {
        NiFiFlowPath flowPath = flowPaths.computeIfAbsent(pathId, k -> new NiFiFlowPath(pathId, namespace));
        flowPath.notifyActive();
        return flowPath;
    }

    public NiFiQueue getOrCreateQueue(String destinationComponentId) {
        NiFiQueue queue = queues.computeIfAbsent(destinationComponentId, k -> new NiFiQueue(destinationComponentId, namespace));
        queue.notifyActive();
        return queue;
    }

    private void createOrUpdateInputPort(String portId, String portName) {
        NiFiInputPort inputPort = inputPorts.computeIfAbsent(portId, k -> new NiFiInputPort(portId, namespace));
        inputPort.setName(portName);
        inputPort.notifyActive();
    }

    private void createOrUpdateOutputPort(String portId, String portName) {
        NiFiOutputPort outputPort = outputPorts.computeIfAbsent(portId, k -> new NiFiOutputPort(portId, namespace));
        outputPort.setName(portName);
        outputPort.notifyActive();
    }

    public Map<String, NiFiFlowPath> getFlowPaths() {
        return flowPaths;
    }

    public Map<String, NiFiQueue> getQueues() {
        return queues;
    }

    public Map<String, NiFiInputPort> getInputPorts() {
        return inputPorts;
    }

    public Map<String, NiFiOutputPort> getOutputPorts() {
        return outputPorts;
    }

    /**
     * Find the flow_path that contains the specified componentId.
     */
    public NiFiFlowPath findPath(String componentId) {
        for (NiFiFlowPath path: flowPaths.values()) {
            if (path.getProcessComponents().contains(componentId)){
                return path;
            }
        }
        return null;
    }

    /**
     * Determine if a component should be reported as NiFiFlowPath.
     */
    public boolean isProcessComponent(String componentId) {
        return isProcessor(componentId) || isRemoteInputPort(componentId) || isRemoteOutputPort(componentId);
    }

    private boolean isProcessor(String componentId) {
        return processors.containsKey(componentId);
    }

    private boolean isRemoteInputPort(String componentId) {
        return remoteInputPorts.containsKey(componentId);
    }

    private boolean isRemoteOutputPort(String componentId) {
        return remoteOutputPorts.containsKey(componentId);
    }

    public String getProcessComponentName(String componentId) {
        return getProcessComponentName(componentId, () -> "unknown");
    }

    public String getProcessComponentName(String componentId, Supplier<String> unknown) {
        return isProcessor(componentId) ? getProcessors().get(componentId).getName()
                : isRemoteInputPort(componentId) ? getRemoteInputPorts().get(componentId).getName()
                : isRemoteOutputPort(componentId) ? getRemoteOutputPorts().get(componentId).getName() : unknown.get();
    }
}
