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
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.utils.AtlasPathExtractorUtil;
import org.apache.atlas.utils.PathExtractorContext;
import org.apache.hadoop.fs.Path;
import org.apache.nifi.atlas.provenance.AbstractNiFiProvenanceEventAnalyzer;
import org.apache.nifi.atlas.provenance.AnalysisContext;
import org.apache.nifi.atlas.provenance.DataSetRefs;
import org.apache.nifi.provenance.ProvenanceEventRecord;


import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;

/**
 * Analyze a transit URI as an Azure ADLS Gen2 directory (skipping the file name).
 * <p>
 * Atlas entity hierarchy: adls_gen2_directory -> adls_gen2_directory -> ... -> adls_gen2_container -> adls_gen2_account
 * <p>adls_gen2_directory
 * <ul>
 *   <li>qualifiedName=abfs://filesystem@account/path@namespace (example: abfs://myfilesystem@myaccount/mydir1/mydir2/@ns1)
 *   <li>name=directory (example: mydir2)
 * </ul>
 * <p>adls_gen2_container
 * <ul>
 *   <li>qualifiedName=abfs://filesystem@account@namespace (example: abfs://myfilesystem@myaccount@ns1)
 *   <li>name=filesystem (example: myfilesystem)
 * </ul>
 * <p>adls_gen2_account
 * <ul>
 *   <li>qualifiedName=abfs://account@namespace (example: abfs://myaccount@ns1)
 *   <li>name=account (example: myaccount)
 * </ul>
 */
public class AzureADLSDirectory extends AbstractNiFiProvenanceEventAnalyzer {

    public static final String TYPE_DIRECTORY = AtlasPathExtractorUtil.ADLS_GEN2_DIRECTORY;
    public static final String TYPE_CONTAINER = AtlasPathExtractorUtil.ADLS_GEN2_CONTAINER;
    public static final String TYPE_ACCOUNT = AtlasPathExtractorUtil.ADLS_GEN2_ACCOUNT;

    public static final String ATTR_PARENT = AtlasPathExtractorUtil.ATTRIBUTE_PARENT;
    public static final String ATTR_ACCOUNT = AtlasPathExtractorUtil.ATTRIBUTE_ACCOUNT;

    @Override
    public DataSetRefs analyze(AnalysisContext context, ProvenanceEventRecord event) {
        String transitUri = event.getTransitUri();
        if (transitUri == null) {
            return null;
        }

        Path path = new Path(transitUri);

        String namespace = context.getNamespaceResolver().fromHostNames(path.toUri().getHost());

        PathExtractorContext pathExtractorContext = new PathExtractorContext(namespace);
        AtlasEntityWithExtInfo fileEntityExt = AtlasPathExtractorUtil.getPathEntity(path, pathExtractorContext);

        // the last component of the URI is returned as a directory object but in fact it refers the filename
        AtlasEntity fileEntity = fileEntityExt.getEntity();
        AtlasObjectId parentObjectId = (AtlasObjectId) fileEntity.getRelationshipAttribute(ATTR_PARENT);
        if (parentObjectId != null) {
            AtlasEntity parentEntity = pathExtractorContext.getKnownEntities().get(parentObjectId.getUniqueAttributes().get(ATTR_QUALIFIED_NAME));

            if (parentEntity != null) {
                AtlasEntityWithExtInfo parentEntityExt = new AtlasEntityWithExtInfo(parentEntity);

                pathExtractorContext.getKnownEntities().values().stream()
                        .filter(entity -> !entity.getGuid().equals(fileEntity.getGuid()))
                        .filter(entity -> !entity.getGuid().equals(parentEntity.getGuid()))
                        .forEach(parentEntityExt::addReferredEntity);

                return singleDataSetRef(event.getComponentId(), event.getEventType(), parentEntityExt);
            }
        }

        return null;
    }

    @Override
    public String targetTransitUriPattern() {
        return "^abfs(s)?://.+@.+/.+$";
    }
}
