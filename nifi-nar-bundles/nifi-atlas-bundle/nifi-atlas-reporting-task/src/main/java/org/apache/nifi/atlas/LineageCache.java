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
package org.apache.nifi.atlas;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.nifi.atlas.NiFiTypes.RelationshipType;

import java.time.Duration;
import java.util.Optional;

public class LineageCache {

    private static final Duration EXPIRATION_TIME = Duration.ofHours(1);

    private static final int CACHE_SIZE = 5_000;

    private static final Object CACHE_PAYLOAD = new Object();

    private final Cache<String, String> dataSetCache = Caffeine.newBuilder()
            .maximumSize(CACHE_SIZE)
            .expireAfterAccess(EXPIRATION_TIME)
            .build();

    private final Cache<String, Object> relationshipCache = Caffeine.newBuilder()
            .maximumSize(CACHE_SIZE)
            .expireAfterAccess(EXPIRATION_TIME)
            .build();

    void addDataSetGuid(String typedQualifiedName, String guid) {
        dataSetCache.put(typedQualifiedName, guid);
    }

    Optional<String> getDataSetGuid(String typedQualifiedName) {
        return Optional.ofNullable(dataSetCache.getIfPresent(typedQualifiedName));
    }

    void addRelationship(RelationshipType relationshipType, String flowPathGuid, String dataSetGuid) {
        relationshipCache.put(getRelationshipKey(relationshipType, flowPathGuid, dataSetGuid), CACHE_PAYLOAD);
    }

    boolean containsRelationship(RelationshipType relationshipType, String flowPathGuid, String dataSetGuid) {
        return relationshipCache.getIfPresent(getRelationshipKey(relationshipType, flowPathGuid, dataSetGuid)) != null;
    }

    private String getRelationshipKey(RelationshipType relationshipType, String flowPathGuid, String dataSetGuid) {
        return String.format("%s_%s_%s", relationshipType.getCode(), flowPathGuid, dataSetGuid);
    }
}
