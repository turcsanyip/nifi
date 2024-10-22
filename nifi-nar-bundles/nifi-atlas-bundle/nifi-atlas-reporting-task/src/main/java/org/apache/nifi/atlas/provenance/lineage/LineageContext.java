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
import org.apache.nifi.atlas.model.NiFiFlowPath;
import org.apache.nifi.atlas.provenance.DataSet;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.nifi.atlas.AtlasUtils.getQualifiedName;
import static org.apache.nifi.atlas.AtlasUtils.getTypedQualifiedName;

public class LineageContext {

    // Map[FlowPath qualifiedName -> FlowPath]
    private final Map<String, NiFiFlowPath> flowPaths = new HashMap<>();

    // Map[DataSet typedQualifiedName -> DataSet]
    private final Map<String, DataSet> dataSets = new HashMap<>();

    // Map[FlowPath qualifiedName -> Set[DataSet typedQualifiedName]]
    private final Map<String, Set<String>> flowPathInputs = new HashMap<>();
    private final Map<String, Set<String>> flowPathOutputs = new HashMap<>();

    public Map<String, NiFiFlowPath> getFlowPaths() {
        return flowPaths;
    }

    public Map<String, DataSet> getDataSets() {
        return dataSets;
    }

    public Map<String, Set<String>> getFlowPathInputs() {
        return flowPathInputs;
    }

    public Map<String, Set<String>> getFlowPathOutputs() {
        return flowPathOutputs;
    }

    public void addFlowPathInput(NiFiFlowPath flowPath, DataSet input) {
        addFlowPathDataSet(flowPath, input, true);
    }

    public void addFlowPathOutput(NiFiFlowPath flowPath, DataSet output) {
        addFlowPathDataSet(flowPath, output, false);
    }

    private void addFlowPathDataSet(NiFiFlowPath flowPath, DataSet dataSet, boolean isInput) {
        String flowPathQualifiedName = flowPath.getQualifiedName();
        flowPaths.put(flowPathQualifiedName, flowPath);

        String dataSetTypedQualifiedName = dataSet.getTypedQualifiedName();
        dataSets.put(dataSetTypedQualifiedName, dataSet);

        Set<String> flowPathDataSets = isInput
                ? flowPathInputs.computeIfAbsent(flowPathQualifiedName, k -> new HashSet<>())
                : flowPathOutputs.computeIfAbsent(flowPathQualifiedName, k -> new HashSet<>());

        flowPathDataSets.add(dataSetTypedQualifiedName);
    }

    @Override
    public String toString() {
        return "LineageContext{" +
                "flowPaths=" + flowPathsToString() +
                ", dataSets=" + dataSetsToString() +
                ", flowPathInputs=" + flowPathInputs +
                ", flowPathOutputs=" + flowPathOutputs +
                '}';
    }

    private String flowPathsToString() {
        return flowPaths.values().stream()
                .map(fp -> fp.getQualifiedName() + "/" + fp.getGuid())
                .collect(Collectors.joining(",", "[", "]"));
    }

    private String dataSetsToString() {
        return dataSets.values().stream()
                .map(ds -> ds.getTypedQualifiedName() + "/" + ds.getGuid())
                .collect(Collectors.joining(",", "[", "]"));
    }
}
