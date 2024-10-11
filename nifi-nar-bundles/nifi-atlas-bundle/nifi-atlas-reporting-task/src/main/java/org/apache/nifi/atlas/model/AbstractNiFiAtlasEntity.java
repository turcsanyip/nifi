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
package org.apache.nifi.atlas.model;

import org.apache.atlas.model.instance.AtlasEntity;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.nifi.atlas.AtlasUtils.getComponentIdFromQualifiedName;
import static org.apache.nifi.atlas.AtlasUtils.getQualifiedName;
import static org.apache.nifi.atlas.AtlasUtils.toQualifiedName;
import static org.apache.nifi.atlas.AtlasUtils.toStr;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_DESCRIPTION;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;

class AbstractNiFiAtlasEntity implements NiFiAtlasEntity {

    private final String id;

    private final AtlasEntity atlasEntity;

    private EntityState state;

    protected final List<String> updateAudit = new ArrayList<>(); // TODO: check what were added

    protected AbstractNiFiAtlasEntity(String typeName, String id, String namespace) {
        this.id = id;
        this.atlasEntity = new AtlasEntity(typeName, ATTR_QUALIFIED_NAME, toQualifiedName(namespace, id));

        notifyCreated();
    }

    protected AbstractNiFiAtlasEntity(AtlasEntity atlasEntity) {
        this.id = getComponentIdFromQualifiedName(getQualifiedName(atlasEntity));
        this.atlasEntity = atlasEntity;

        notifyFetched();
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public AtlasEntity getAtlasEntity() {
        return atlasEntity;
    }

    @Override
    public String getGuid() {
        return atlasEntity.getGuid();
    }

    @Override
    public void setGuid(String guid) {
        atlasEntity.setGuid(guid);
    }

    public String getName() {
        return getStringAttribute(ATTR_NAME);
    }

    public void setName(String name) {
        setStringAttribute(ATTR_NAME, name);
    }

    public String getDescription() {
        return getStringAttribute(ATTR_DESCRIPTION);
    }

    public void setDescription(String description) {
        setStringAttribute(ATTR_DESCRIPTION, description);
    }

    @Override
    public EntityState getState() {
        return state;
    }

    protected void notifyFetched() {
        state = NiFiAtlasEntity.EntityState.FETCHED;
    }

    protected void notifyCreated() {
        state = NiFiAtlasEntity.EntityState.CREATED;
    }

    protected void notifyUpdated() {
        if (state == NiFiAtlasEntity.EntityState.FETCHED || (state == NiFiAtlasEntity.EntityState.AS_IS)) {
            state = NiFiAtlasEntity.EntityState.UPDATED;
        }
    }

    protected void notifyActive() {
        if (state == NiFiAtlasEntity.EntityState.FETCHED) {
            state = NiFiAtlasEntity.EntityState.AS_IS;
        }
    }

    protected String getStringAttribute(String attributeName) {
        return toStr(getAttribute(attributeName));
    }

    protected void setStringAttribute(String attributeName, String newValue) {
        setAttribute(attributeName, newValue);
    }

    protected Object getAttribute(String attributeName) {
        return atlasEntity.getAttribute(attributeName);
    }

    protected void setAttribute(String attributeName, Object newValue) {
        final Object currentValue = getAttribute(attributeName);
        if (isChanged(currentValue, newValue)) {
            atlasEntity.setAttribute(attributeName, newValue);

            notifyUpdated();
            updateAudit.add(String.format("%s changed from %s to %s", attributeName, currentValue, newValue));
        }
    }

    protected void removeAttribute(String attributeName) {
        atlasEntity.removeAttribute(attributeName);
    }

    protected List<Map<String, String>> getRelationshipAttribute(String attributeName) {
        return (List) atlasEntity.getRelationshipAttribute(attributeName);
    }

    protected void removeRelationshipAttributes() {
        atlasEntity.setRelationshipAttributes(null);
    }

    private boolean isChanged(Object currentValue, Object newValue) {
        if (currentValue == null) {
            return newValue != null;
        } else {
            return !currentValue.equals(newValue);
        }
    }

    public List<String> getUpdateAudit() {
        return updateAudit;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null) {
            return false;
        }

        if (getClass() != obj.getClass()) {
            return false;
        }

        AbstractNiFiAtlasEntity that = (AbstractNiFiAtlasEntity) obj;

        return id.equals(that.id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

}
