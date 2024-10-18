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
package org.apache.nifi.atlas.provenance;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class DataSetRefs {
    private final String componentId;
    private Set<DataSet> inputs;
    private Set<DataSet> outputs;

    public DataSetRefs(String componentId) {
        this.componentId = componentId;
    }

    public String getComponentId() {
        return componentId;
    }

    public Set<DataSet> getInputs() {
        return inputs != null ? inputs : Collections.emptySet();
    }

    public void addInput(DataSet input) {
        if (inputs == null) {
            inputs = new HashSet<>();
        }
        inputs.add(input);
    }

    public Set<DataSet> getOutputs() {
        return outputs != null ? outputs : Collections.emptySet();
    }

    public void addOutput(DataSet output) {
        if (outputs == null) {
            outputs = new HashSet<>();
        }
        outputs.add(output);
    }

    public boolean isEmpty() {
        return (inputs == null || inputs.isEmpty()) && (outputs == null || outputs.isEmpty());
    }

    @Override
    public String toString() {
        return "DataSetRefs{" +
                "componentId='" + componentId + '\'' +
                ", inputs=" + inputs +
                ", outputs=" + outputs +
                '}';
    }
}
