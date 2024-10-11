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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.nifi.atlas.NiFiTypes.ATTR_URL;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_FLOW_PATH;

public class NiFiFlowPath extends NiFiComponent {

    private final List<String> processComponentIds = new ArrayList<>();

    private final Map<NiFiQueue, RelationshipInfo> inputQueues = new HashMap<>();
    private final Map<NiFiQueue, RelationshipInfo> outputQueues = new HashMap<>();

    public NiFiFlowPath(String componentId, String namespace) {
        super(TYPE_NIFI_FLOW_PATH, componentId, namespace);
    }

    public NiFiFlowPath(String componentId, long lineageHash, String namespace) {
        super(TYPE_NIFI_FLOW_PATH, componentId + "::" + lineageHash, namespace);
    }

    public NiFiFlowPath(AtlasEntity flowPathEntity) {
        super(flowPathEntity);
    }

    public String getUrl() {
        return getStringAttribute(ATTR_URL);
    }

    public void setUrl(String url) {
        setStringAttribute(ATTR_URL, url);
    }

    public void addProcessComponent(String processorId) {
        processComponentIds.add(processorId);
    }

    public List<String> getProcessComponents() {
        return processComponentIds;
    }

    void addInputQueue(NiFiQueue queue, String relationshipGuid) {
        inputQueues.put(queue, new RelationshipInfo(relationshipGuid));
    }

    void addOutputQueue(NiFiQueue queue, String relationshipGuid) {
        outputQueues.put(queue, new RelationshipInfo(relationshipGuid));
    }

    public void connectInputQueue(NiFiQueue queue) {
        final RelationshipInfo relationshipInfo = inputQueues.get(queue);
        if (relationshipInfo == null) {
            inputQueues.put(queue, new RelationshipInfo());
        } else {
            relationshipInfo.notifyActive();
        }
    }

    public void connectOutputQueue(NiFiQueue queue) {
        final RelationshipInfo relationshipInfo = outputQueues.get(queue);
        if (relationshipInfo == null) {
            outputQueues.put(queue, new RelationshipInfo());
        } else {
            relationshipInfo.notifyActive();
        }
    }

    public Map<NiFiQueue, RelationshipInfo> getInputQueues() {
        return inputQueues;
    }

    public Map<NiFiQueue, RelationshipInfo> getOutputQueues() {
        return outputQueues;
    }

    public static String createDeepLinkUrl(String nifiUrl, String groupId, String flowPathId) {
        // Remove lineage hash part of the flow path id in case of complete path lineage strategy.
        final String componentId = flowPathId.split("::")[0];
        return String.format("%s?processGroupId=%s&componentIds=%s", nifiUrl, groupId, componentId);
    }

    @Override
    public String toString() {
        return "NiFiFlowPath{" +
                "name='" + getName() + '\'' +
                ", processComponentIds=" + processComponentIds +
                '}';
    }
}
