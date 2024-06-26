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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.nifi.atlas.AtlasUtils.getQualifiedName;
import static org.apache.nifi.atlas.AtlasUtils.getTypedQualifiedName;

public class LineageContext {

    // Map[FlowPath qualifiedName -> FlowPath entity]
    private final Map<String, AtlasEntity> flowPaths = new HashMap<>();

    // Map[DataSet typedQualifiedName -> DataSet entity]
    private final Map<String, AtlasEntityWithExtInfo> dataSets = new HashMap<>();

    // Map[FlowPath qualifiedName -> Set[DataSet typedQualifiedName]]
    private final Map<String, Set<String>> flowPathInputs = new HashMap<>();
    private final Map<String, Set<String>> flowPathOutputs = new HashMap<>();

    // Map[FlowPath qualifiedName -> Set[DataSet typedQualifiedName]]
    private final Map<String, Set<String>> savedFlowPathInputs = new HashMap<>();
    private final Map<String, Set<String>> savedFlowPathOutputs = new HashMap<>();

    public Map<String, AtlasEntity> getFlowPaths() {
        return flowPaths;
    }

    public Map<String, AtlasEntityWithExtInfo> getDataSets() {
        return dataSets;
    }

    public Map<String, Set<String>> getFlowPathInputs() {
        return flowPathInputs;
    }

    public Map<String, Set<String>> getFlowPathOutputs() {
        return flowPathOutputs;
    }

    public void addFlowPathInput(AtlasEntity flowPath, AtlasEntityWithExtInfo input) {
        addFlowPathDataSet(flowPath, input, true);
    }

    public void addFlowPathOutput(AtlasEntity flowPath, AtlasEntityWithExtInfo output) {
        addFlowPathDataSet(flowPath, output, false);
    }

    private void addFlowPathDataSet(AtlasEntity flowPath, AtlasEntityWithExtInfo dataSet, boolean isInput) {
        String flowPathQualifiedName = getQualifiedName(flowPath);
        flowPaths.putIfAbsent(flowPathQualifiedName, flowPath);

        String dataSetTypedQualifiedName = getTypedQualifiedName(dataSet.getEntity());
        dataSets.putIfAbsent(dataSetTypedQualifiedName, dataSet);

        if (!isFlowPathDataSetAlreadyAdded(flowPathQualifiedName, dataSetTypedQualifiedName, isInput)) {
            Set<String> flowPathDataSets = isInput
                    ? flowPathInputs.computeIfAbsent(flowPathQualifiedName, k -> new HashSet<>())
                    : flowPathOutputs.computeIfAbsent(flowPathQualifiedName, k -> new HashSet<>());

            flowPathDataSets.add(dataSetTypedQualifiedName);
        }
    }

    private boolean isFlowPathDataSetAlreadyAdded(String flowPathQualifiedName, String dataSetTypedQualifiedName, boolean isInput) {
        if (isInput) {
            return isFlowPathDataSetAlreadyAdded(flowPathQualifiedName, dataSetTypedQualifiedName, flowPathInputs)
                    || isFlowPathDataSetAlreadyAdded(flowPathQualifiedName, dataSetTypedQualifiedName, savedFlowPathInputs);
        } else {
            return isFlowPathDataSetAlreadyAdded(flowPathQualifiedName, dataSetTypedQualifiedName, flowPathOutputs)
                    || isFlowPathDataSetAlreadyAdded(flowPathQualifiedName, dataSetTypedQualifiedName, savedFlowPathOutputs);
        }
    }

    private boolean isFlowPathDataSetAlreadyAdded(String flowPathQualifiedName, String dataSetTypedQualifiedName, Map<String, Set<String>> flowPathDataSets) {
        return flowPathDataSets.getOrDefault(flowPathQualifiedName, Collections.emptySet()).contains(dataSetTypedQualifiedName);
    }

    public void commit() {
        flowPathInputs.forEach((flowPath, inputs) ->
                savedFlowPathInputs.computeIfAbsent(flowPath, k -> new HashSet<>())
                        .addAll(inputs));

        flowPathOutputs.forEach((flowPath, outputs) ->
                savedFlowPathOutputs.computeIfAbsent(flowPath, k -> new HashSet<>())
                        .addAll(outputs));

        flowPathInputs.clear();
        flowPathOutputs.clear();
    }

    @Override
    public String toString() {
        return "LineageContext{" +
                "flowPaths=" + flowPathsToString() +
                ", dataSets=" + dataSetsToString() +
                ", flowPathInputs=" + flowPathInputs +
                ", flowPathOutputs=" + flowPathOutputs +
                ", savedFlowPathInputs=" + savedFlowPathInputs +
                ", savedFlowPathOutputs=" + savedFlowPathOutputs +
                '}';
    }

    private String flowPathsToString() {
        return flowPaths.values().stream()
                .map(e -> getQualifiedName(e) + "/" + e.getGuid())
                .collect(Collectors.joining(",", "[", "]"));
    }

    private String dataSetsToString() {
        return dataSets.values().stream()
                .map(AtlasEntityWithExtInfo::getEntity)
                .map(e -> getTypedQualifiedName(e) + "/" + e.getGuid())
                .collect(Collectors.joining(",", "[", "]"));
    }
}
