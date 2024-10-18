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

import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.nifi.atlas.provenance.AbstractNiFiProvenanceEventAnalyzer;
import org.apache.nifi.atlas.provenance.DataSet;
import org.apache.nifi.util.Tuple;

import static org.apache.nifi.atlas.AtlasUtils.toQualifiedName;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_CLUSTER_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;
import static org.apache.nifi.atlas.provenance.analyzer.DatabaseAnalyzerUtil.toTableNameStr;

public abstract class AbstractHiveAnalyzer extends AbstractNiFiProvenanceEventAnalyzer {

    static final String TYPE_DATABASE = "hive_db";
    static final String TYPE_TABLE = "hive_table";
    static final String ATTR_DB = "db";

    private DataSet createDatabaseDataSet(String namespace, String databaseName) {
        final DataSet databaseDataSet = new DataSet(TYPE_DATABASE);
        databaseDataSet.setAttribute(ATTR_NAME, databaseName);
        // The attribute 'clusterName' is in the 'hive_db' Atlas entity so it cannot be changed.
        //  Using 'namespace' as value for lack of better solution.
        databaseDataSet.setAttribute(ATTR_CLUSTER_NAME, namespace);
        databaseDataSet.setAttribute(ATTR_QUALIFIED_NAME, toQualifiedName(namespace, databaseName));
        return databaseDataSet;
    }

    protected DataSet createTableDataSet(String namespace, Tuple<String, String> tableName) {
        final DataSet databaseDataSet = createDatabaseDataSet(namespace, tableName.getKey());

        final DataSet tableDataSet = new DataSet(TYPE_TABLE);

        tableDataSet.setAttribute(ATTR_NAME, tableName.getValue());
        tableDataSet.setAttribute(ATTR_QUALIFIED_NAME, toQualifiedName(namespace, toTableNameStr(tableName)));
        tableDataSet.setAttribute(ATTR_DB, new AtlasObjectId(databaseDataSet.getGuid()));

        tableDataSet.addReferredEntity(databaseDataSet);

        return tableDataSet;
    }

}
