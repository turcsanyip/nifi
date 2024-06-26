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

/**
 * Analyze a transit URI as an AWS S3 directory (skipping the object name).
 * The analyzer outputs a v1 or v2 AWS S3 directory entity depending on the 'AWS S3 Model Version' property configured on the reporting task.
 * <p>
 * Atlas entity hierarchy v1: aws_s3_pseudo_dir -> aws_s3_bucket
 * <p>aws_s3_pseudo_dir
 * <ul>
 *   <li>qualifiedName=s3a://bucket/path@namespace (example: s3a://mybucket/mydir1/mydir2@ns1)
 *   <li>name=/path (example: /mydir1/mydir2)
 * </ul>
 * <p>aws_s3_bucket
 * <ul>
 *   <li>qualifiedName=s3a://bucket@namespace (example: s3a://mybucket@ns1)
 *   <li>name=bucket (example: mybucket)
 * </ul>
 * <p>
 * Atlas entity hierarchy v2: aws_s3_v2_directory -> aws_s3_v2_directory -> ... -> aws_s3_v2_bucket
 * <p>aws_s3_v2_directory
 * <ul>
 *   <li>qualifiedName=s3a://bucket/path/@namespace (example: s3a://mybucket/mydir1/mydir2/@ns1)
 *   <li>name=directory (example: mydir2)
 * </ul>
 * <p>aws_s3_v2_bucket
 * <ul>
 *   <li>qualifiedName=s3a://bucket@namespace (example: s3a://mybucket@ns1)
 *   <li>name=bucket (example: mybucket)
 * </ul>
 */
public class AwsS3Directory extends AbstractDirectoryAnalyzer {

    @Override
    public String targetTransitUriPattern() {
        return "^s3a://.+/.+$";
    }

}
