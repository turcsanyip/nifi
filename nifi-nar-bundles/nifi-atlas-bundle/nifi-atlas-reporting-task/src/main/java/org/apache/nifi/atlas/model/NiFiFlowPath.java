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
import org.apache.nifi.atlas.AtlasProcess;
import org.apache.nifi.atlas.AtlasUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.apache.nifi.atlas.AtlasUtils.getTypedQualifiedName;
import static org.apache.nifi.atlas.AtlasUtils.updateMetadata;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_DESCRIPTION;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_URL;

public class NiFiFlowPath implements AtlasProcess {
    private final List<String> processComponentIds = new ArrayList<>();

    private final String id;
    private final Set<AtlasObjectId> inputs = new HashSet<>();
    private final Set<AtlasObjectId> outputs = new HashSet<>();

    private String name;
    private String description;
    private String url;

    private AtlasEntity atlasEntity;

    private final AtomicBoolean metadataUpdated = new AtomicBoolean(false);
    private final List<String> updateAudit = new ArrayList<>();
    private Set<String> existingInputGuids;
    private Set<String> existingOutputGuids;

    private Set<String> existingInputTypedQualifiedNames;
    private Set<String> existingOutputTypedQualifiedNames;


    public NiFiFlowPath(String id) {
        this.id = id;
    }
    public NiFiFlowPath(String id, long lineageHash) {
        this.id =  id + "::" + lineageHash;
    }

    public AtlasEntity getAtlasEntity() {
        return atlasEntity;
    }

    public void setAtlasEntity(AtlasEntity atlasEntity) {
        this.atlasEntity = atlasEntity;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        updateMetadata(metadataUpdated, updateAudit, ATTR_NAME, this.name, name);
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        updateMetadata(metadataUpdated, updateAudit, ATTR_DESCRIPTION, this.description, description);
        this.description = description;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        updateMetadata(metadataUpdated, updateAudit, ATTR_URL, this.url, url);
        this.url = url;
    }

    public void addProcessComponent(String processorId) {
        processComponentIds.add(processorId);
    }

    public Set<AtlasObjectId> getInputs() {
        return inputs;
    }

    public Set<AtlasObjectId> getOutputs() {
        return outputs;
    }

    public boolean hasInput(AtlasEntity entity) {
        return existingInputTypedQualifiedNames!= null && existingInputTypedQualifiedNames.contains(getTypedQualifiedName(entity));
    }

    public boolean hasOutput(AtlasEntity entity) {
        return existingOutputTypedQualifiedNames != null && existingOutputTypedQualifiedNames.contains(getTypedQualifiedName(entity));
    }

    public List<String> getProcessComponentIds() {
        return processComponentIds;
    }

    public String getId() {
        return id;
    }

    public static String createDeepLinkUrl(String nifiUrl, String groupId, String flowPathId) {
        // Remove lineage hash part of the flow path id in case of complete path lineage strategy.
        final String componentId = flowPathId.split("::")[0];
        return String.format("%s?processGroupId=%s&componentIds=%s", nifiUrl, groupId, componentId);
    }

    /**
     * Start tracking changes from current state.
     */
    public void startTrackingChanges(NiFiFlow nifiFlow) {
        this.metadataUpdated.set(false);
        this.updateAudit.clear();
        existingInputGuids = inputs.stream().map(AtlasObjectId::getGuid).collect(Collectors.toSet());
        existingOutputGuids = outputs.stream().map(AtlasObjectId::getGuid).collect(Collectors.toSet());

        existingInputTypedQualifiedNames = inputs.stream().map(AtlasUtils::getTypedQualifiedName).collect(Collectors.toSet());
        existingOutputTypedQualifiedNames = outputs.stream().map(AtlasUtils::getTypedQualifiedName).collect(Collectors.toSet());

        // Remove all nifi_queues those are owned by the nifiFlow to delete ones no longer exist.
        // Because it should be added again if not deleted when flow analysis finished.
        final Set<AtlasObjectId> ownedQueues = nifiFlow.getQueues().keySet();
        inputs.removeAll(ownedQueues);
        outputs.removeAll(ownedQueues);
    }

    public boolean isMetadataUpdated() {
        return this.metadataUpdated.get();
    }

    public List<String> getUpdateAudit() {
        return updateAudit;
    }

    boolean isDataSetReferenceChanged(Set<AtlasObjectId> ids, boolean isInput) {
        final Set<String> guids = ids.stream().map(AtlasObjectId::getGuid).collect(Collectors.toSet());
        final Set<String> existingGuids = isInput ? existingInputGuids : existingOutputGuids;
        return existingGuids == null || !existingGuids.equals(guids);
    }

    @Override
    public String toString() {
        return "NiFiFlowPath{" +
                "name='" + name + '\'' +
                ", processComponentIds=" + processComponentIds +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        NiFiFlowPath that = (NiFiFlowPath) o;

        return id.equals(that.id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }
}
