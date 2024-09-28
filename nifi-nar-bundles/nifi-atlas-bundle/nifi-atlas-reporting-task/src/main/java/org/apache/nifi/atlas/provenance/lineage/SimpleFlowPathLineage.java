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
package org.apache.nifi.atlas.provenance.lineage;

import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.nifi.atlas.NiFiFlow;
import org.apache.nifi.atlas.NiFiFlowPath;
import org.apache.nifi.atlas.provenance.AnalysisContext;
import org.apache.nifi.atlas.provenance.DataSetRefs;
import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.lineage.ComputeLineageResult;
import org.apache.nifi.provenance.lineage.LineageEdge;
import org.apache.nifi.provenance.lineage.LineageNode;
import org.apache.nifi.provenance.lineage.LineageNodeType;

import java.util.List;

import static org.apache.nifi.atlas.NiFiFlowPath.createDeepLinkUrl;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_QUEUE;

public class SimpleFlowPathLineage extends AbstractLineageStrategy {

    @Override
    public void processEvent(AnalysisContext analysisContext, LineageContext lineageContext, NiFiFlow nifiFlow, ProvenanceEventRecord event) {
        final DataSetRefs refs = executeAnalyzer(analysisContext, event);
        if (refs == null || (refs.isEmpty())) {
            return;
        }

        if ("Remote Input Port".equals(event.getComponentType()) || "Remote Output Port".equals(event.getComponentType())) {
            // handling the client side of the S2S connection (not where the Remote Port component resides)
            processRemotePortEvent(analysisContext, lineageContext, nifiFlow, event, refs);
        } else {
            addDataSetRefs(lineageContext, nifiFlow, refs);
        }

    }

    /**
     * Create a flow_path entity corresponding to the target RemoteGroupPort when a SEND/RECEIVE event are received.
     * Because such entity can not be created in advance while analyzing flow statically,
     * as ReportingTask can not determine whether a component id is a RemoteGroupPort,
     * since connectionStatus is the only available information in ReportingContext.
     * ConnectionStatus only knows component id, component type is unknown.
     * For example, there is no difference to tell if a connected component is a funnel or a RemoteGroupPort.
     */
    private void processRemotePortEvent(AnalysisContext analysisContext, LineageContext lineageContext, NiFiFlow nifiFlow, ProvenanceEventRecord event, DataSetRefs analyzedRefs) {

        final boolean isRemoteInputPort = "Remote Input Port".equals(event.getComponentType());

        // Create a RemoteInputPort Process.
        // event.getComponentId returns UUID for RemoteGroupPort as a client of S2S, and it's different from a remote port UUID (portDataSetid).
        // See NIFI-4571 for detail.
        final AtlasEntityWithExtInfo remotePortDataSet = isRemoteInputPort ? analyzedRefs.getOutputs().iterator().next() : analyzedRefs.getInputs().iterator().next();
        final String portProcessId = event.getComponentId();

        final NiFiFlowPath remotePortProcess = new NiFiFlowPath(portProcessId);
        remotePortProcess.setName(event.getComponentType());
        remotePortProcess.addProcessComponent(portProcessId);

        // For RemoteInputPort, need to find the previous component connected to this port,
        // which passed this particular FlowFile.
        // That is only possible by calling lineage API.
        if (isRemoteInputPort) {
            final ProvenanceEventRecord previousEvent = findPreviousProvenanceEvent(analysisContext, event);
            if (previousEvent == null) {
                logger.warn("Previous event was not found: {}", event);
                return;
            }

            final List<ConnectionStatus> incomingConnections = nifiFlow.getIncomingConnections(portProcessId);
            if (incomingConnections == null || incomingConnections.isEmpty()) {
                logger.warn("Incoming relationship was not found: {}", event);
                return;
            }

            // Set link to the first incoming connection.
            final ConnectionStatus firstConnection = incomingConnections.get(0);
            remotePortProcess.setUrl(createDeepLinkUrl(nifiFlow.getUrl(), firstConnection.getGroupId(), firstConnection.getId()));

            // Create a queue.
            AtlasEntity queueFromStaticFlowPathToRemotePortProcessEntity = new AtlasEntity(TYPE_NIFI_QUEUE);
            queueFromStaticFlowPathToRemotePortProcessEntity.setAttribute(ATTR_NAME, "queue");
            queueFromStaticFlowPathToRemotePortProcessEntity.setAttribute(ATTR_QUALIFIED_NAME, nifiFlow.toQualifiedName(portProcessId));

            // Create lineage: Static flow_path -> queue
            DataSetRefs staticFlowPathRefs = new DataSetRefs(previousEvent.getComponentId());
            staticFlowPathRefs.addOutput(queueFromStaticFlowPathToRemotePortProcessEntity);
            addDataSetRefs(lineageContext, nifiFlow, staticFlowPathRefs);


            // Create lineage: Queue -> RemoteInputPort process -> RemoteInputPort dataSet
            DataSetRefs remotePortRefs = new DataSetRefs(portProcessId);
            remotePortRefs.addInput(queueFromStaticFlowPathToRemotePortProcessEntity);
            remotePortRefs.addOutput(remotePortDataSet);
            addDataSetRefs(lineageContext, nifiFlow, remotePortProcess, remotePortRefs);

        } else {
            // For RemoteOutputPort, it's possible that multiple processors are connected.
            // In that case, the received FlowFile is cloned and passed to each connection.
            // So we need to create multiple DataSetRefs.
            final List<ConnectionStatus> connections = nifiFlow.getOutgoingConnections(portProcessId);
            if (connections == null || connections.isEmpty()) {
                logger.warn("Outgoing connection was not found: {}", event);
                return;
            }

            // Set link to the first outgoing connection.
            final ConnectionStatus firstConnection = connections.get(0);
            remotePortProcess.setUrl(createDeepLinkUrl(nifiFlow.getUrl(), firstConnection.getGroupId(), firstConnection.getId()));

            // Create lineage: RemoteOutputPort dataSet -> RemoteOutputPort process
            DataSetRefs remotePortRefs = new DataSetRefs(portProcessId);
            remotePortRefs.addInput(remotePortDataSet);
            addDataSetRefs(lineageContext, nifiFlow, remotePortProcess, remotePortRefs);

            for (ConnectionStatus connection : connections) {
                final String destinationId = connection.getDestinationId();
                final NiFiFlowPath destFlowPath = nifiFlow.findPath(destinationId);
                if (destFlowPath == null) {
                    // If the destination of a connection is a Remote Input Port,
                    // then its corresponding flow path may not be created yet.
                    // In such direct RemoteOutputPort to RemoteInputPort case, do not add a queue from this RemoteOutputPort
                    // as a queue will be created by the connected RemoteInputPort to connect this RemoteOutputPort.
                    continue;
                }

                // Create a queue.
                AtlasEntity queueFromRemotePortProcessToStaticFlowPathEntity = new AtlasEntity(TYPE_NIFI_QUEUE);
                queueFromRemotePortProcessToStaticFlowPathEntity.setAttribute(ATTR_NAME, "queue");
                queueFromRemotePortProcessToStaticFlowPathEntity.setAttribute(ATTR_QUALIFIED_NAME, nifiFlow.toQualifiedName(destinationId));

                // Create lineage: Queue -> Static flow_path
                DataSetRefs staticFlowPathRefs = new DataSetRefs(destinationId);
                staticFlowPathRefs.addInput(queueFromRemotePortProcessToStaticFlowPathEntity);
                addDataSetRefs(lineageContext, nifiFlow, staticFlowPathRefs);

                // Create lineage: RemoteOutputPort dataSet -> RemoteOutputPort process -> Queue
                remotePortRefs.addOutput(queueFromRemotePortProcessToStaticFlowPathEntity);
                addDataSetRefs(lineageContext, nifiFlow, remotePortProcess, remotePortRefs);
            }

            // Add RemoteOutputPort process, so that it can be found even if it is connected to RemoteInputPort directory without any processor in between.
            nifiFlow.getFlowPaths().put(remotePortProcess.getId(), remotePortProcess);

        }

    }

    private ProvenanceEventRecord findPreviousProvenanceEvent(AnalysisContext context, ProvenanceEventRecord event) {
        final ComputeLineageResult lineage = context.queryLineage(event.getEventId());
        if (lineage == null) {
            logger.warn("Lineage was not found: {}", event);
            return null;
        }

        // If no previous provenance node found due to expired or other reasons, just log a warning msg and do nothing.
        final LineageNode previousProvenanceNode = traverseLineage(lineage, String.valueOf(event.getEventId()));
        if (previousProvenanceNode == null) {
            logger.warn("Traverse lineage could not find any preceding provenance event node: {}", event);
            return null;
        }

        final long previousEventId = Long.parseLong(previousProvenanceNode.getIdentifier());
        return context.getProvenanceEvent(previousEventId);
    }

    /**
     * Recursively traverse lineage graph until a preceding provenance event is found.
     */
    private LineageNode traverseLineage(ComputeLineageResult lineage, String eventId) {
        final LineageNode previousNode = lineage.getEdges().stream()
                .filter(edge -> edge.getDestination().getIdentifier().equals(String.valueOf(eventId)))
                .findFirst().map(LineageEdge::getSource).orElse(null);
        if (previousNode == null) {
            return null;
        }
        if (previousNode.getNodeType().equals(LineageNodeType.PROVENANCE_EVENT_NODE)) {
            return previousNode;
        }
        return traverseLineage(lineage, previousNode.getIdentifier());
    }


}
