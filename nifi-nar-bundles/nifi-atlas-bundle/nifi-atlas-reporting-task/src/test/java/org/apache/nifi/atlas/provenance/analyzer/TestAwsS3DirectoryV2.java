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

import org.apache.atlas.utils.AtlasPathExtractorUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.atlas.provenance.DataSet;
import org.apache.nifi.atlas.provenance.DataSetRefs;
import org.junit.jupiter.api.Test;

import static org.apache.nifi.atlas.NiFiTypes.ATTR_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;
import static org.apache.nifi.atlas.provenance.analyzer.AnalyzerTestUtils.getReferredDataSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class TestAwsS3DirectoryV2 extends AbstractTestAwsS3Directory {

    private static final String TYPE_DIRECTORY_V2 = AtlasPathExtractorUtil.AWS_S3_V2_PSEUDO_DIR;
    private static final String TYPE_BUCKET_V2 = AtlasPathExtractorUtil.AWS_S3_V2_BUCKET;
    private static final String ATTR_CONTAINER_V2 = AtlasPathExtractorUtil.ATTRIBUTE_CONTAINER;
    private static final String ATTR_OBJECT_PREFIX_V2 = AtlasPathExtractorUtil.ATTRIBUTE_OBJECT_PREFIX;

    @Override
    protected String getAwsS3ModelVersion() {
        return AtlasPathExtractorUtil.AWS_S3_ATLAS_MODEL_VERSION_V2;
    }

    @Test
    public void testSimpleDirectory() {
        String processorName = "PutHDFS";
        String dirPath = "/dir1";

        executeTest(processorName, dirPath);
    }

    @Test
    public void testCompoundDirectory() {
        String processorName = "PutHDFS";
        String dirPath = "/dir1/dir2/dir3/dir4/dir5";

        executeTest(processorName, dirPath);
    }

    @Test
    public void testRootDirectory() {
        String processorName = "PutHDFS";
        String dirPath = "/";

        executeTest(processorName, dirPath);
    }

    @Test
    public void testWithPutORC() {
        String processorName = "PutORC";
        String dirPath = "/dir1";

        executeTest(processorName, dirPath);
    }

    protected void assertAnalysisResult(DataSetRefs refs, String dirPath) {
        assertEquals(0, refs.getInputs().size());
        assertEquals(1, refs.getOutputs().size());

        DataSet dataSet = refs.getOutputs().iterator().next();

        DataSet actualDataSet = dataSet;
        String actualPath = dirPath;
        while (StringUtils.isNotEmpty(actualPath) && !"/".equals(actualPath)) {
            String directory = StringUtils.substringAfterLast(actualPath, "/");

            assertEquals(TYPE_DIRECTORY_V2, actualDataSet.getTypeName());
            assertEquals(String.format("s3a://%s%s/@%s", AWS_BUCKET, actualPath, ATLAS_NAMESPACE), actualDataSet.getAttribute(ATTR_QUALIFIED_NAME));
            assertEquals(directory, actualDataSet.getAttribute(ATTR_NAME));
            assertEquals(actualPath + "/", actualDataSet.getAttribute(ATTR_OBJECT_PREFIX_V2));
            assertNotNull(actualDataSet.getRelationshipAttribute(ATTR_CONTAINER_V2));

            actualDataSet = getReferredDataSet(dataSet, actualDataSet.getRelationshipAttribute(ATTR_CONTAINER_V2));
            actualPath = StringUtils.substringBeforeLast(actualPath, "/");
        }

        assertEquals(TYPE_BUCKET_V2, actualDataSet.getTypeName());
        assertEquals(String.format("s3a://%s@%s", AWS_BUCKET, ATLAS_NAMESPACE), actualDataSet.getAttribute(ATTR_QUALIFIED_NAME));
        assertEquals(AWS_BUCKET, actualDataSet.getAttribute(ATTR_NAME));
    }
}
