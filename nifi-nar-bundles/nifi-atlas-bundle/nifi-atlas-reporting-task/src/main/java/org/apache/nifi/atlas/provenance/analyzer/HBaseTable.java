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
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityExtInfo;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.nifi.atlas.provenance.AbstractNiFiProvenanceEventAnalyzer;
import org.apache.nifi.atlas.provenance.AnalysisContext;
import org.apache.nifi.atlas.provenance.DataSetRefs;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.nifi.atlas.AtlasUtils.toQualifiedName;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_CLUSTER_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_URI;

/**
 * Analyze a transit URI as a HBase table.
 * <li>qualifiedName=hbaseNamespace:tableName@namespace (example: default:myTable@ns1)
 * <li>name=[hbaseNamespace:]tableName (example: myTable)
 */
public class HBaseTable extends AbstractNiFiProvenanceEventAnalyzer {

    private static final Logger logger = LoggerFactory.getLogger(HBaseTable.class);

    static final String TYPE_HBASE_TABLE = "hbase_table";
    static final String TYPE_HBASE_NAMESPACE = "hbase_namespace";

    static final String ATTR_NAMESPACE = "namespace";

    static final String DEFAULT_NAMESPACE = "default";

    // hbase://masterAddress/[hbaseNamespace:]hbaseTableName/hbaseRowId(optional)
    private static final Pattern URI_PATTERN = Pattern.compile("^hbase://([^/]+)/(([^/]+):)?([^/]+)/?.*$");

    @Override
    public DataSetRefs analyze(AnalysisContext context, ProvenanceEventRecord event) {

        final String transitUri = event.getTransitUri();
        final Matcher uriMatcher = URI_PATTERN.matcher(transitUri);
        if (!uriMatcher.matches()) {
            logger.warn("Unexpected transit URI: {}", transitUri);
            return null;
        }

        final String[] hostNames = splitHostNames(uriMatcher.group(1));
        final String namespace = context.getNamespaceResolver().fromHostNames(hostNames);

        final String hbaseNamespaceName = uriMatcher.group(3) != null ? uriMatcher.group(3) : DEFAULT_NAMESPACE;
        final String hbaseTableName = uriMatcher.group(4);

        final AtlasEntity hbaseNamespaceEntity = createHBaseNamespaceEntity(namespace, hbaseNamespaceName);
        final AtlasEntityWithExtInfo hbaseTableEntityExt = getHBaseTableEntity(namespace, hbaseTableName, hbaseNamespaceEntity);

        return singleDataSetRef(event.getComponentId(), event.getEventType(), hbaseTableEntityExt);
    }

    @Override
    public String targetTransitUriPattern() {
        return "^hbase://.+$";
    }

    private AtlasEntity createHBaseNamespaceEntity(String namespace, String hbaseNamespaceName) {
        final AtlasEntity hbaseNamespaceEntity = new AtlasEntity(TYPE_HBASE_NAMESPACE);

        hbaseNamespaceEntity.setAttribute(ATTR_NAME, hbaseNamespaceName);
        hbaseNamespaceEntity.setAttribute(ATTR_QUALIFIED_NAME, toQualifiedName(namespace, hbaseNamespaceName));
        hbaseNamespaceEntity.setAttribute(ATTR_CLUSTER_NAME, namespace);

        return hbaseNamespaceEntity;
    }

    private AtlasEntityWithExtInfo getHBaseTableEntity(String namespace, String hbaseTableName, AtlasEntity hbaseNamespaceEntity) {
        final AtlasEntity hbaseTableEntity = new AtlasEntity(TYPE_HBASE_TABLE);

        final String hbaseTableFullName = String.format("%s:%s", hbaseNamespaceEntity.getAttribute(ATTR_NAME), hbaseTableName);
        final boolean isDefaultHBaseNamespace = DEFAULT_NAMESPACE.equals(hbaseNamespaceEntity.getAttribute(ATTR_NAME));

        hbaseTableEntity.setAttribute(ATTR_NAME, isDefaultHBaseNamespace ? hbaseTableName : hbaseTableFullName);
        hbaseTableEntity.setAttribute(ATTR_QUALIFIED_NAME, toQualifiedName(namespace, hbaseTableFullName));
        hbaseTableEntity.setAttribute(ATTR_NAMESPACE, new AtlasObjectId(hbaseNamespaceEntity.getGuid()));
        hbaseTableEntity.setAttribute(ATTR_URI, isDefaultHBaseNamespace ? hbaseTableName : hbaseTableFullName);

        return new AtlasEntityWithExtInfo(hbaseTableEntity, new AtlasEntityExtInfo(hbaseNamespaceEntity));
    }
}
