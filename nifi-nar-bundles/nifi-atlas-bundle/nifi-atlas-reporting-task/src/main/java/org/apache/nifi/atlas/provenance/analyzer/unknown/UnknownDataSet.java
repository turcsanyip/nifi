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
package org.apache.nifi.atlas.provenance.analyzer.unknown;

import org.apache.nifi.atlas.provenance.AbstractNiFiProvenanceEventAnalyzer;
import org.apache.nifi.atlas.provenance.AnalysisContext;
import org.apache.nifi.atlas.provenance.DataSet;
import org.apache.nifi.provenance.ProvenanceEventRecord;

import static org.apache.nifi.atlas.AtlasUtils.toQualifiedName;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_DESCRIPTION;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_DATA;

public abstract class UnknownDataSet extends AbstractNiFiProvenanceEventAnalyzer {

    protected static final String TYPE = TYPE_NIFI_DATA;

    protected DataSet createDataSet(AnalysisContext context, ProvenanceEventRecord event) {
        final DataSet dataSet = new DataSet(TYPE);
        dataSet.setAttribute(ATTR_NAME, event.getComponentType());
        dataSet.setAttribute(ATTR_QUALIFIED_NAME, toQualifiedName(context.getNiFiNamespace(), event.getComponentId()));
        dataSet.setAttribute(ATTR_DESCRIPTION, event.getEventType() + " was performed by " + event.getComponentType());
        return dataSet;
    }

}
