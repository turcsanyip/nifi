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

import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.nifi.atlas.AtlasUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DataSet {

    private final AtlasEntity entity;

    private List<AtlasEntity> referredEntities;

    public DataSet(String typeName) {
        this.entity = new AtlasEntity(typeName);
    }

    public DataSet(AtlasEntity entity) {
        this.entity = entity;
    }

    public AtlasEntity getEntity() {
        return entity;
    }

    public void addReferredEntity(AtlasEntity entity) {
        if (referredEntities == null) {
            referredEntities = new ArrayList<>();
        }

        referredEntities.add(entity);
    }

    public void addReferredEntity(DataSet dataSet) {
        addReferredEntity(dataSet.getEntity());
    }

    public List<AtlasEntity> getReferredEntities() {
        return referredEntities != null ? referredEntities : Collections.emptyList();
    }

    public String getGuid() {
        return entity.getGuid();
    }

    public void setGuid(String guid) {
        entity.setGuid(guid);
    }

    public boolean isGuidNotAssigned() {
        return !AtlasUtils.isGuidAssigned(getGuid());
    }

    public String getTypedQualifiedName() {
        return AtlasUtils.getTypedQualifiedName(entity);
    }

    public void setAttribute(String name, Object value) {
        entity.setAttribute(name, value);
    }

    public Object getAttribute(String name) {
        return entity.getAttribute(name);
    }
}
