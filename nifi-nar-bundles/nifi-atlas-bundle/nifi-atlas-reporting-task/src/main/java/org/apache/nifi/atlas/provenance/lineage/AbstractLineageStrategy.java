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
import org.apache.nifi.atlas.model.NiFiFlow;
import org.apache.nifi.atlas.model.NiFiFlowPath;
import org.apache.nifi.atlas.provenance.AnalysisContext;
import org.apache.nifi.atlas.provenance.DataSetRefs;
import org.apache.nifi.atlas.provenance.NiFiProvenanceEventAnalyzer;
import org.apache.nifi.atlas.provenance.NiFiProvenanceEventAnalyzerFactory;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.nifi.atlas.NiFiTypes.ATTR_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_NIFI_FLOW;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_URL;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_FLOW_PATH;

public abstract class AbstractLineageStrategy implements LineageStrategy {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    protected DataSetRefs executeAnalyzer(AnalysisContext analysisContext, ProvenanceEventRecord event) {
        final NiFiProvenanceEventAnalyzer analyzer = NiFiProvenanceEventAnalyzerFactory.getAnalyzer(event.getComponentType(), event.getTransitUri(), event.getEventType());
        if (analyzer == null) {
            return null;
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Analyzer {} is found for event: {}", analyzer, event);
        }
        return analyzer.analyze(analysisContext, event);
    }

    protected void addDataSetRefs(LineageContext lineageContext, NiFiFlow nifiFlow, DataSetRefs refs) {
        final String componentId = refs.getComponentId();
        final NiFiFlowPath flowPath = nifiFlow.findPath(componentId);
        if (flowPath == null) {
            logger.warn("FlowPath for {} was not found.", componentId);
            return;
        }

        addDataSetRefs(lineageContext, flowPath, refs);
    }

    protected void addDataSetRefs(LineageContext lineageContext, NiFiFlowPath flowPath, DataSetRefs refs) {
        logger.debug("Adding DataSetRefs: {} {}", flowPath, refs);

        final AtlasEntity flowPathEntity = flowPath.getAtlasEntity(); // TODO: NiFiFlowPath

        for (AtlasEntityWithExtInfo input: refs.getInputs()) { // TODO: DataSet
            lineageContext.addFlowPathInput(flowPathEntity, input);
        }

        for (AtlasEntityWithExtInfo output: refs.getOutputs()) {
            lineageContext.addFlowPathOutput(flowPathEntity, output);
        }
    }
}
