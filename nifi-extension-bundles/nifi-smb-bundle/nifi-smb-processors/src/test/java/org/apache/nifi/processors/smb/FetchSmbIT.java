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
package org.apache.nifi.processors.smb;

import static org.apache.nifi.processors.smb.FetchSmb.COMPLETION_STRATEGY;
import static org.apache.nifi.processors.smb.FetchSmb.CREATE_DESTINATION_DIRECTORY;
import static org.apache.nifi.processors.smb.FetchSmb.DESTINATION_DIRECTORY;
import static org.apache.nifi.processors.smb.FetchSmb.REL_FAILURE;
import static org.apache.nifi.processors.smb.FetchSmb.REL_SUCCESS;
import static org.apache.nifi.processors.smb.FetchSmb.REMOTE_FILE;
import static org.apache.nifi.util.TestRunners.newTestRunner;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.processors.smb.util.CompletionStrategy;
import org.apache.nifi.services.smb.SmbjClientProviderService;
import org.apache.nifi.util.TestRunner;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FetchSmbIT extends SambaTestContainers {

    private static final String TEST_CONTENT = "test_content";

    private TestRunner testRunner;

    private SmbjClientProviderService smbjClientProviderService;

    @BeforeEach
    void setUpComponents() throws Exception {
        testRunner = newTestRunner(FetchSmb.class);

        smbjClientProviderService = configureSmbClient(testRunner, true);
    }

    @AfterEach
    void tearDownComponents() {
        testRunner.disableControllerService(smbjClientProviderService);
    }

    @Test
    void fetchFilesUsingEL() {
        writeFile("test_file", TEST_CONTENT);

        testRunner.setProperty(REMOTE_FILE, "${attribute_to_find_using_EL}");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("attribute_to_find_using_EL", "test_file");

        runProcessor(attributes);

        assertSuccessFlowFile();
    }

    @Test
    void tryToFetchNonExistingFileEmitsFailure() throws Exception {
        testRunner.setProperty(REMOTE_FILE, "${attribute_to_find_using_EL}");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("attribute_to_find_using_EL", "non_existing_file");

        runProcessor(attributes);

        testRunner.assertTransferCount(REL_FAILURE, 1);
    }

    @Test
    void testCompletionStrategyNone() {
        createDirectory("dir", AccessMode.READ_ONLY);
        writeFile("dir/test_file", TEST_CONTENT, AccessMode.READ_ONLY);

        testRunner.setProperty(REMOTE_FILE, "dir/test_file");
        testRunner.setProperty(COMPLETION_STRATEGY, CompletionStrategy.NONE);

        runProcessor();

        assertSuccessFlowFile();

        assertTrue(fileExists("dir/test_file"));
    }

    @Test
    void testCompletionStrategyDelete() {
        createDirectory("dir", AccessMode.READ_WRITE);
        writeFile("dir/test_file", TEST_CONTENT, AccessMode.READ_WRITE);

        testRunner.setProperty(REMOTE_FILE, "dir/test_file");
        testRunner.setProperty(COMPLETION_STRATEGY, CompletionStrategy.DELETE);

        runProcessor();

        testRunner.assertTransferCount(REL_SUCCESS, 1);
        assertEquals("test_content", testRunner.getFlowFilesForRelationship(REL_SUCCESS).get(0).getContent());
        assertFalse(fileExists("dir/test_file"));
    }

    @Test
    void testCompletionStrategyMoveWithExistingDirectory() {
        createDirectory("dir", AccessMode.READ_WRITE);
        writeFile("dir/test_file", TEST_CONTENT, AccessMode.READ_WRITE);
        createDirectory("processed", AccessMode.READ_WRITE);

        testRunner.setProperty(REMOTE_FILE, "dir/test_file");
        testRunner.setProperty(COMPLETION_STRATEGY, CompletionStrategy.MOVE);
        testRunner.setProperty(DESTINATION_DIRECTORY, "processed");

        runProcessor();

        assertSuccessFlowFile();

        assertFalse(fileExists("dir/test_file"));
        assertTrue(fileExists("processed/test_file"));
    }

    @Test
    void testCompletionStrategyMoveWithCreatingDirectory() {
        createDirectory("dir", AccessMode.READ_WRITE);
        writeFile("dir/test_file", TEST_CONTENT, AccessMode.READ_WRITE);

        testRunner.setProperty(REMOTE_FILE, "dir/test_file");
        testRunner.setProperty(COMPLETION_STRATEGY, CompletionStrategy.MOVE);
        testRunner.setProperty(DESTINATION_DIRECTORY, "processed");
        testRunner.setProperty(CREATE_DESTINATION_DIRECTORY, "true");

        runProcessor();

        assertSuccessFlowFile();

        assertFalse(fileExists("dir/test_file"));
        assertTrue(fileExists("processed/test_file"));
    }

    @Test
    void testCompletionStrategyDeleteFailsWhenNoPermission() {
        createDirectory("dir", AccessMode.READ_ONLY);
        writeFile("dir/test_file", TEST_CONTENT, AccessMode.READ_ONLY);

        testRunner.setProperty(REMOTE_FILE, "dir/test_file");
        testRunner.setProperty(COMPLETION_STRATEGY, CompletionStrategy.DELETE);

        runProcessor();

        assertSuccessFlowFile();
        assertFalse(testRunner.getLogger().getWarnMessages().isEmpty());

        assertTrue(fileExists("dir/test_file"));
    }

    @Test
    void testCompletionStrategyMoveFailsWhenNoPermission() {
        createDirectory("dir", AccessMode.READ_ONLY);
        writeFile("dir/test_file", TEST_CONTENT, AccessMode.READ_ONLY);
        createDirectory("processed", AccessMode.READ_ONLY);

        testRunner.setProperty(REMOTE_FILE, "dir/test_file");
        testRunner.setProperty(COMPLETION_STRATEGY, CompletionStrategy.MOVE);
        testRunner.setProperty(DESTINATION_DIRECTORY, "processed");

        runProcessor();

        assertSuccessFlowFile();
        assertFalse(testRunner.getLogger().getWarnMessages().isEmpty());

        assertTrue(fileExists("dir/test_file"));
        assertFalse(fileExists("processed/test_file"));
    }

    private void runProcessor() {
        runProcessor(Collections.emptyMap());
    }

    private void runProcessor(final Map<String, String> attributes) {
        testRunner.enqueue("ignored", attributes);
        testRunner.run();
    }

    private void assertSuccessFlowFile() {
        testRunner.assertTransferCount(REL_SUCCESS, 1);
        assertEquals(TEST_CONTENT, testRunner.getFlowFilesForRelationship(REL_SUCCESS).get(0).getContent());
    }

}
