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
package org.apache.nifi.atlas.provenance.analyzer;

import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.utils.AtlasPathExtractorUtil;
import org.apache.atlas.utils.PathExtractorContext;
import org.apache.hadoop.fs.Path;
import org.apache.nifi.atlas.provenance.AbstractNiFiProvenanceEventAnalyzer;
import org.apache.nifi.atlas.provenance.AnalysisContext;
import org.apache.nifi.atlas.provenance.DataSet;
import org.apache.nifi.atlas.provenance.DataSetRefs;
import org.apache.nifi.provenance.ProvenanceEventRecord;

public abstract class AbstractDirectoryAnalyzer extends AbstractNiFiProvenanceEventAnalyzer {

    @Override
    public DataSetRefs analyze(AnalysisContext context, ProvenanceEventRecord event) {
        String transitUri = event.getTransitUri();
        if (transitUri == null) {
            return null;
        }

        String directoryUri = transitUri.substring(0, transitUri.lastIndexOf('/') + 1);

        Path path = new Path(directoryUri);

        String namespace = context.getNamespaceResolver().fromHostNames(path.toUri().getHost());

        PathExtractorContext pathExtractorContext = new PathExtractorContext(namespace, context.getAwsS3ModelVersion());
        AtlasEntity.AtlasEntityWithExtInfo pathEntityExt = AtlasPathExtractorUtil.getPathEntity(path, pathExtractorContext);

        DataSet directoryDataSet = new DataSet(pathEntityExt.getEntity());

        pathExtractorContext.getKnownEntities().values().stream()
                .filter(entity -> !entity.getGuid().equals(directoryDataSet.getGuid()))
                .forEach(directoryDataSet::addReferredEntity);

        return singleDataSetRef(event.getComponentId(), event.getEventType(), directoryDataSet);
    }
}
