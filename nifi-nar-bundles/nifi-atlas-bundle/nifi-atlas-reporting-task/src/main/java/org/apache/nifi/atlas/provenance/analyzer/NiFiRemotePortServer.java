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

import org.apache.nifi.atlas.provenance.AnalysisContext;
import org.apache.nifi.atlas.provenance.DataSet;
import org.apache.nifi.atlas.provenance.DataSetRefs;
import org.apache.nifi.controller.status.PortStatus;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

import static org.apache.nifi.atlas.AtlasUtils.toQualifiedName;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_INPUT_PORT;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_OUTPUT_PORT;

/**
 * Analyze a provenance event as a NiFi Remote Port for Site-to-Site communication at the server side.
 * <li>qualifiedName=remotePortID (example: 35dbc0ab-015e-1000-144c-a8d71255027d)
 * <li>name=portName (example: input)
 */
public class NiFiRemotePortServer extends NiFiS2S {

    private static final Logger logger = LoggerFactory.getLogger(NiFiRemotePortServer.class);

    @Override
    public DataSetRefs analyze(AnalysisContext context, ProvenanceEventRecord event) {

        if (!ProvenanceEventType.SEND.equals(event.getEventType())
                && !ProvenanceEventType.RECEIVE.equals(event.getEventType())) {
            return null;
        }

        final URI uri = parseTransitUri(event);
        final String namespace = getNamespace(context, uri);

        final boolean isInputPort = event.getComponentType().equals("Input Port");
        final String type = isInputPort ? TYPE_NIFI_INPUT_PORT : TYPE_NIFI_OUTPUT_PORT;

        final String remotePortId = event.getComponentId();

        // Find remote port.
        final PortStatus remotePort = isInputPort
                ? context.getRemoteInputPort(remotePortId)
                : context.getRemoteOutputPort(remotePortId);
        if (remotePort == null) {
            logger.warn("Remote Port was not found: {}", event);
            return null;
        }
        final String remotePortName = remotePort.getName();

        final DataSet dataSet = new DataSet(type);
        dataSet.setAttribute(ATTR_NAME, remotePortName);
        dataSet.setAttribute(ATTR_QUALIFIED_NAME, toQualifiedName(namespace, remotePortId));

        return singleDataSetRef(remotePortId, event.getEventType(), dataSet);
    }

    @Override
    public String targetComponentTypePattern() {
        return "^(In|Out)put Port$";
    }
}
