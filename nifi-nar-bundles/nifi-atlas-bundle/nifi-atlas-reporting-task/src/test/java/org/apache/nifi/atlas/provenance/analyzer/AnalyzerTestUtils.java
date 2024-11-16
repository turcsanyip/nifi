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
import org.apache.nifi.atlas.provenance.DataSet;
import org.opentest4j.AssertionFailedError;

class AnalyzerTestUtils {

    private AnalyzerTestUtils() {
    }

    static DataSet getReferredDataSet(DataSet dataSet, Object reference) {
        String guid;
        if (reference instanceof AtlasObjectId) {
            guid = ((AtlasObjectId) reference).getGuid();
        } else {
            throw new IllegalArgumentException("Referred DataSet cannot be obtained by reference: " + reference);
        }

        for (DataSet referredDataSet : dataSet.getReferredDataSets()) {
            if (referredDataSet.getGuid().equals(guid)) {
                return referredDataSet;
            }
        }

        throw new AssertionFailedError("Referred DataSet not found by reference: " + reference);
    }
}
