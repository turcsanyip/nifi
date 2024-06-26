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

import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.SearchFilter;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasRelationship;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.commons.collections4.ListUtils;
import org.apache.nifi.atlas.provenance.lineage.LineageContext;
import org.apache.nifi.util.StringUtils;
import org.apache.nifi.util.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.MultivaluedMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.nifi.atlas.AtlasUtils.findIdByQualifiedName;
import static org.apache.nifi.atlas.AtlasUtils.getComponentIdFromQualifiedName;
import static org.apache.nifi.atlas.AtlasUtils.getQualifiedName;
import static org.apache.nifi.atlas.AtlasUtils.getTypedQualifiedName;
import static org.apache.nifi.atlas.AtlasUtils.isGuidAssigned;
import static org.apache.nifi.atlas.AtlasUtils.toStr;
import static org.apache.nifi.atlas.NiFiFlow.EntityChangeType.AS_IS;
import static org.apache.nifi.atlas.NiFiFlow.EntityChangeType.CREATED;
import static org.apache.nifi.atlas.NiFiFlow.EntityChangeType.DELETED;
import static org.apache.nifi.atlas.NiFiFlow.EntityChangeType.UPDATED;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_DESCRIPTION;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_FLOW_PATHS;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_GUID;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_INPUTS;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_INPUT_PORTS;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_OUTPUTS;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_OUTPUT_PORTS;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUEUES;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_RELATIONSHIP_STATUS;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_TYPENAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_URL;
import static org.apache.nifi.atlas.NiFiTypes.ENTITIES;
import static org.apache.nifi.atlas.NiFiTypes.REL_PROCESS_INPUT;
import static org.apache.nifi.atlas.NiFiTypes.REL_PROCESS_OUTPUT;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_FLOW;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_FLOW_PATH;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_INPUT_PORT;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_OUTPUT_PORT;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_QUEUE;

public class NiFiAtlasClient implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(NiFiAtlasClient.class);

    private static final int CREATE_BATCH_SIZE = 100;

    private final AtlasClientV2 atlasClient;

    public NiFiAtlasClient(AtlasClientV2 atlasClient) {
        this.atlasClient = atlasClient;
    }

    @Override
    public void close() {
        atlasClient.close();
    }

    /**
     * This is an utility method to delete unused types.
     * Should be used during development or testing only.
     * @param typeNames to delete
     */
    void deleteTypeDefs(String ... typeNames) throws AtlasServiceException {
        final AtlasTypesDef existingTypeDef = getTypeDefs(typeNames);
        try {
            atlasClient.deleteAtlasTypeDefs(existingTypeDef);
        } catch (UniformInterfaceException e) {
            if (e.getResponse().getStatus() == 204) {
                // 204 is a successful response.
                // NOTE: However after executing this, Atlas should be restarted to work properly.
                logger.info("Deleted type defs: {}", existingTypeDef);
            } else {
                throw e;
            }
        }
    }

    /**
     * @return True when required NiFi types are already created.
     */
    public boolean isNiFiTypeDefsRegistered() throws AtlasServiceException {
        final Set<String> typeNames = ENTITIES.keySet();
        final Map<String, AtlasEntityDef> existingDefs = getTypeDefs(typeNames.toArray(new String[typeNames.size()])).getEntityDefs().stream()
                .collect(Collectors.toMap(AtlasEntityDef::getName, Function.identity()));
        return typeNames.stream().allMatch(existingDefs::containsKey);
    }

    /**
     * Create or update NiFi types in Atlas type system.
     * @param update If false, doesn't perform anything if there is existing type def for the name.
     */
    public void registerNiFiTypeDefs(boolean update) throws AtlasServiceException {
        final Set<String> typeNames = ENTITIES.keySet();
        final Map<String, AtlasEntityDef> existingDefs = getTypeDefs(typeNames.toArray(new String[typeNames.size()])).getEntityDefs().stream()
                .collect(Collectors.toMap(AtlasEntityDef::getName, Function.identity()));


        final AtomicBoolean shouldUpdate = new AtomicBoolean(false);

        final AtlasTypesDef type = new AtlasTypesDef();

        typeNames.stream().filter(typeName -> {
            final AtlasEntityDef existingDef = existingDefs.get(typeName);
            if (existingDef != null) {
                // type is already defined.
                if (!update) {
                    return false;
                }
                shouldUpdate.set(true);
            }
            return true;
        }).forEach(typeName -> {
            final NiFiTypes.EntityDefinition def = ENTITIES.get(typeName);

            final AtlasEntityDef entity = new AtlasEntityDef();
            type.getEntityDefs().add(entity);

            entity.setName(typeName);

            Set<String> superTypes = new HashSet<>();
            List<AtlasAttributeDef> attributes = new ArrayList<>();

            def.define(entity, superTypes, attributes);

            entity.setSuperTypes(superTypes);
            entity.setAttributeDefs(attributes);
        });

        // Create or Update.
        final AtlasTypesDef atlasTypeDefsResult = shouldUpdate.get()
                ? atlasClient.updateAtlasTypeDefs(type)
                : atlasClient.createAtlasTypeDefs(type);
        logger.debug("Result={}", atlasTypeDefsResult);
    }

    private AtlasTypesDef getTypeDefs(String ... typeNames) throws AtlasServiceException {
        final AtlasTypesDef typeDefs = new AtlasTypesDef();
        for (int i = 0; i < typeNames.length; i++) {
            final MultivaluedMap<String, String> searchParams = new MultivaluedMapImpl();
            searchParams.add(SearchFilter.PARAM_NAME, typeNames[i]);
            final AtlasTypesDef typeDef = atlasClient.getAllTypeDefs(new SearchFilter(searchParams));
            typeDefs.getEntityDefs().addAll(typeDef.getEntityDefs());
        }
        logger.debug("typeDefs={}", typeDefs);
        return typeDefs;
    }

    private Pattern FLOW_PATH_URL_PATTERN = Pattern.compile("^http.+processGroupId=([0-9a-z\\-]+).*$");
    /**
     * Fetch existing NiFiFlow entity from Atlas.
     * @param rootProcessGroupId The id of a NiFi flow root process group.
     * @param namespace The namespace of a flow.
     * @return A NiFiFlow instance filled with retrieved data from Atlas. Status objects are left blank, e.g. ProcessorStatus.
     * @throws AtlasServiceException Thrown if requesting to Atlas API failed, including when the flow is not found.
     */
    public NiFiFlow fetchNiFiFlow(String rootProcessGroupId, String namespace) throws AtlasServiceException {

        final String qualifiedName = AtlasUtils.toQualifiedName(namespace, rootProcessGroupId);
        final AtlasObjectId flowId = new AtlasObjectId(TYPE_NIFI_FLOW, ATTR_QUALIFIED_NAME, qualifiedName);
        final AtlasEntity.AtlasEntityWithExtInfo nifiFlowExt = searchEntityDef(flowId);

        if (nifiFlowExt == null || nifiFlowExt.getEntity() == null) {
            return null;
        }

        final AtlasEntity nifiFlowEntity = nifiFlowExt.getEntity();
        final Map<String, AtlasEntity> nifiFlowReferredEntities = nifiFlowExt.getReferredEntities();
        final Map<String, Object> attributes = nifiFlowEntity.getAttributes();
        final NiFiFlow nifiFlow = new NiFiFlow(rootProcessGroupId);
        nifiFlow.setAtlasEntity(nifiFlowEntity);
        nifiFlow.setFlowName(toStr(attributes.get(ATTR_NAME)));
        nifiFlow.setNamespace(namespace);
        nifiFlow.setUrl(toStr(attributes.get(ATTR_URL)));
        nifiFlow.setDescription(toStr(attributes.get(ATTR_DESCRIPTION)));

        nifiFlow.getQueues().putAll(fetchFlowComponents(TYPE_NIFI_QUEUE, nifiFlowReferredEntities));
        nifiFlow.getRootInputPortEntities().putAll(fetchFlowComponents(TYPE_NIFI_INPUT_PORT, nifiFlowReferredEntities));
        nifiFlow.getRootOutputPortEntities().putAll(fetchFlowComponents(TYPE_NIFI_OUTPUT_PORT, nifiFlowReferredEntities));

        final Map<String, NiFiFlowPath> flowPaths = nifiFlow.getFlowPaths();
        final Map<AtlasObjectId, AtlasEntity> flowPathEntities = fetchFlowComponents(TYPE_NIFI_FLOW_PATH, nifiFlowReferredEntities);

        for (AtlasEntity flowPathEntity : flowPathEntities.values()) {
            final String pathQualifiedName = getQualifiedName(flowPathEntity);
            final NiFiFlowPath flowPath = new NiFiFlowPath(getComponentIdFromQualifiedName(pathQualifiedName));
            if (flowPathEntity.hasAttribute(ATTR_URL)) {
                final Matcher urlMatcher = FLOW_PATH_URL_PATTERN.matcher(toStr(flowPathEntity.getAttribute(ATTR_URL)));
                if (urlMatcher.matches()) {
                    flowPath.setGroupId(urlMatcher.group(1));
                }
            }
            flowPath.setAtlasEntity(flowPathEntity);
            flowPath.setName(toStr(flowPathEntity.getAttribute(ATTR_NAME)));
            flowPath.getInputs().addAll(toAtlasObjectIds(flowPathEntity.getRelationshipAttribute(ATTR_INPUTS)));
            flowPath.getOutputs().addAll(toAtlasObjectIds(flowPathEntity.getRelationshipAttribute(ATTR_OUTPUTS)));
            flowPath.startTrackingChanges(nifiFlow);

            flowPaths.put(flowPath.getId(), flowPath);
        }

        nifiFlow.startTrackingChanges();
        return nifiFlow;
    }

    /**
     * Retrieves the flow components of type {@code componentType} from Atlas server.
     * Deleted components will be filtered out before calling Atlas.
     * Atlas object ids will be initialized with all the attributes (guid, type, unique attributes) in order to be able
     * to match ids retrieved from Atlas (having guid) and ids created by the reporting task (not having guid yet).
     *
     * @param componentType Atlas type of the flow component (nifi_flow_path, nifi_queue, nifi_input_port, nifi_output_port)
     * @param referredEntities referred entities of the flow entity (returned when the flow fetched) containing the basic data (id, status) of the flow components
     * @return flow component entities mapped to their object ids
     */
    private Map<AtlasObjectId, AtlasEntity> fetchFlowComponents(String componentType, Map<String, AtlasEntity> referredEntities) {
        return referredEntities.values().stream()
                .filter(referredEntity -> referredEntity.getTypeName().equals(componentType))
                .filter(referredEntity -> referredEntity.getStatus() == AtlasEntity.Status.ACTIVE)
                .map(referredEntity -> {
                    final Map<String, Object> uniqueAttributes = Collections.singletonMap(ATTR_QUALIFIED_NAME, getQualifiedName(referredEntity));
                    final AtlasObjectId id = new AtlasObjectId(referredEntity.getGuid(), componentType, uniqueAttributes);
                    try {
                        final AtlasEntity.AtlasEntityWithExtInfo fetchedEntityExt = searchEntityDef(id);
                        return new Tuple<>(id, fetchedEntityExt.getEntity());
                    } catch (AtlasServiceException e) {
                        logger.warn("Failed to search entity by id {}, due to {}", id, e);
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toMap(Tuple::getKey, Tuple::getValue));
    }

    /**
     * Retrieves AtlasObjectIds for the input/output relationships of a NiFiFlowPath.
     * Also filters out the deleted relationships.
     *
     * @param _relationships input/output relationships of a NiFiFlowPath
     * @return AtlasObjectIds of the active relationships
     */
    @SuppressWarnings("unchecked")
    private List<AtlasObjectId> toAtlasObjectIds(Object _relationships) {
        if (_relationships == null) {
            return Collections.emptyList();
        }
        List<Map<String, Object>> relationships = (List<Map<String, Object>>) _relationships;
        return relationships.stream()
                .filter(rel -> rel.get(ATTR_RELATIONSHIP_STATUS).equals(AtlasRelationship.Status.ACTIVE.name()))
                .map(rel -> new AtlasObjectId(toStr(rel.get(ATTR_GUID)), toStr(rel.get(ATTR_TYPENAME)), Collections.singletonMap(ATTR_QUALIFIED_NAME, rel.get(ATTR_QUALIFIED_NAME))))
                .collect(Collectors.toList());
    }

    public void registerNiFiFlow(NiFiFlow nifiFlow) throws AtlasServiceException {

        // Create parent flow entity, so that common properties are taken over.
        final AtlasEntity flowEntity = registerNiFiFlowEntity(nifiFlow);

        // Create DataSet entities those are created by this NiFi flow.
        final Map<String, List<AtlasEntity>> updatedDataSetEntities = registerDataSetEntities(nifiFlow);

        // Create path entities.
        final Set<AtlasObjectId> remainingPathIds = registerFlowPathEntities(nifiFlow);

        // Update these attributes only if anything is created, updated or removed.
        boolean shouldUpdateNiFiFlow = nifiFlow.isMetadataUpdated();
        if (remainingPathIds != null) {
            flowEntity.setAttribute(ATTR_FLOW_PATHS, remainingPathIds);
            shouldUpdateNiFiFlow = true;
        }
        if (updatedDataSetEntities.containsKey(TYPE_NIFI_QUEUE)) {
            flowEntity.setAttribute(ATTR_QUEUES, updatedDataSetEntities.get(TYPE_NIFI_QUEUE));
            shouldUpdateNiFiFlow = true;
        }
        if (updatedDataSetEntities.containsKey(TYPE_NIFI_INPUT_PORT)) {
            flowEntity.setAttribute(ATTR_INPUT_PORTS, updatedDataSetEntities.get(TYPE_NIFI_INPUT_PORT));
            shouldUpdateNiFiFlow = true;
        }
        if (updatedDataSetEntities.containsKey(TYPE_NIFI_OUTPUT_PORT)) {
            flowEntity.setAttribute(ATTR_OUTPUT_PORTS, updatedDataSetEntities.get(TYPE_NIFI_OUTPUT_PORT));
            shouldUpdateNiFiFlow = true;
        }

        if (logger.isDebugEnabled()) {
            logger.debug("### NiFi Flow Audit Logs START");
            nifiFlow.getUpdateAudit().forEach(logger::debug);
            nifiFlow.getFlowPaths().forEach((k, v) -> {
                logger.debug("--- NiFiFlowPath Audit Logs: {}", k);
                v.getUpdateAudit().forEach(logger::debug);
            });
            logger.debug("### NiFi Flow Audit Logs END");
        }

        if (shouldUpdateNiFiFlow) {
            // Send updated entities.
            final List<AtlasEntity> entities = new ArrayList<>();
            final AtlasEntity.AtlasEntitiesWithExtInfo atlasEntities = new AtlasEntity.AtlasEntitiesWithExtInfo(entities);
            entities.add(flowEntity);
            try {
                final EntityMutationResponse mutationResponse = atlasClient.createEntities(atlasEntities);
                logger.debug("mutation response={}", mutationResponse);
            } catch (AtlasServiceException e) {
                if (e.getStatus().getStatusCode() == AtlasErrorCode.INSTANCE_NOT_FOUND.getHttpCode().getStatusCode()
                        && e.getMessage().contains(AtlasErrorCode.INSTANCE_NOT_FOUND.getErrorCode())) {
                    // NOTE: If previously existed nifi_flow_path entity is removed because the path is removed from NiFi,
                    // then Atlas respond with 404 even though the entity is successfully updated.
                    // Following exception is thrown in this case. Just log it.
                    // org.apache.atlas.AtlasServiceException:
                    // Metadata service API org.apache.atlas.AtlasBaseClient$APIInfo@45a37759
                    // failed with status 404 (Not Found) Response Body
                    // ({"errorCode":"ATLAS-404-00-00B","errorMessage":"Given instance is invalid/not found:
                    // Could not find entities in the repository with guids: [96d24487-cd66-4795-b552-f00b426fed26]"})
                    logger.debug("Received error response from Atlas but it should be stored." + e);
                } else {
                    throw e;
                }
            }
        }
    }

    private AtlasEntity registerNiFiFlowEntity(final NiFiFlow nifiFlow) throws AtlasServiceException {
        final List<AtlasEntity> entities = new ArrayList<>();
        final AtlasEntity.AtlasEntitiesWithExtInfo atlasEntities = new AtlasEntity.AtlasEntitiesWithExtInfo(entities);

        if (!nifiFlow.isMetadataUpdated()) {
            // Nothing has been changed, return existing entity.
            return nifiFlow.getAtlasEntity();
        }

        // Create parent flow entity using existing NiFiFlow entity if available, so that common properties are taken over.
        final AtlasEntity flowEntity = nifiFlow.getAtlasEntity() != null ? new AtlasEntity(nifiFlow.getAtlasEntity()) : new AtlasEntity();
        flowEntity.setTypeName(TYPE_NIFI_FLOW);
        flowEntity.setVersion(1L);
        flowEntity.setAttribute(ATTR_NAME, nifiFlow.getFlowName());
        flowEntity.setAttribute(ATTR_QUALIFIED_NAME, nifiFlow.toQualifiedName(nifiFlow.getRootProcessGroupId()));
        flowEntity.setAttribute(ATTR_URL, nifiFlow.getUrl());
        flowEntity.setAttribute(ATTR_DESCRIPTION, nifiFlow.getDescription());

        // If flowEntity is not persisted yet, then store nifi_flow entity to make nifiFlowId available for other entities.
        if (flowEntity.getGuid().startsWith("-")) {
            entities.add(flowEntity);
            final EntityMutationResponse mutationResponse = atlasClient.createEntities(atlasEntities);
            logger.debug("Registered a new nifi_flow entity, mutation response={}", mutationResponse);
            final String assignedNiFiFlowGuid = mutationResponse.getGuidAssignments().get(flowEntity.getGuid());
            flowEntity.setGuid(assignedNiFiFlowGuid);
            nifiFlow.setAtlasGuid(assignedNiFiFlowGuid);
        }

        return flowEntity;
    }

    /**
     * Register DataSet within specified NiFiFlow.
     * @return Set of registered Atlas type names and its remaining entities without deleted ones.
     */
    private Map<String, List<AtlasEntity>> registerDataSetEntities(final NiFiFlow nifiFlow) throws AtlasServiceException {

        final Map<NiFiFlow.EntityChangeType, List<AtlasEntity>> changedEntities = nifiFlow.getChangedDataSetEntities();

        if (changedEntities.containsKey(CREATED)) {
            final List<AtlasEntity> createdEntities = changedEntities.get(CREATED);
            final AtlasEntity.AtlasEntitiesWithExtInfo atlasEntities = new AtlasEntity.AtlasEntitiesWithExtInfo(createdEntities);
            final EntityMutationResponse mutationResponse = atlasClient.createEntities(atlasEntities);
            logger.debug("Created DataSet entities mutation response={}", mutationResponse);

            final Map<String, String> guidAssignments = mutationResponse.getGuidAssignments();
            for (AtlasEntity entity : createdEntities) {

                final String guid = guidAssignments.get(entity.getGuid());
                final String qualifiedName = getQualifiedName(entity);
                if (StringUtils.isEmpty(guid)) {
                    logger.warn("GUID was not assigned for {}::{} for some reason.", entity.getTypeName(), qualifiedName);
                    continue;
                }

                final Map<AtlasObjectId, AtlasEntity> entityMap;
                switch (entity.getTypeName()) {
                    case TYPE_NIFI_INPUT_PORT:
                        entityMap = nifiFlow.getRootInputPortEntities();
                        break;
                    case TYPE_NIFI_OUTPUT_PORT:
                        entityMap = nifiFlow.getRootOutputPortEntities();
                        break;
                    case TYPE_NIFI_QUEUE:
                        entityMap = nifiFlow.getQueues();
                        break;
                    default:
                        throw new RuntimeException(entity.getTypeName() + " is not expected.");
                }


                // In order to replace the id, remove current id which does not have GUID.
                findIdByQualifiedName(entityMap.keySet(), qualifiedName).ifPresent(entityMap::remove);
                entity.setGuid(guid);
                final AtlasObjectId idWithGuid = new AtlasObjectId(guid, entity.getTypeName(), Collections.singletonMap(ATTR_QUALIFIED_NAME, qualifiedName));
                entityMap.put(idWithGuid, entity);
            }
        }

        if (changedEntities.containsKey(UPDATED)) {
            final List<AtlasEntity> updatedEntities = changedEntities.get(UPDATED);
            final AtlasEntity.AtlasEntitiesWithExtInfo atlasEntities = new AtlasEntity.AtlasEntitiesWithExtInfo(updatedEntities);
            final EntityMutationResponse mutationResponse = atlasClient.updateEntities(atlasEntities);
            logger.debug("Updated DataSet entities mutation response={}", mutationResponse);
        }

        final Set<String> changedTypeNames = changedEntities.entrySet().stream()
                .filter(entry -> !AS_IS.equals(entry.getKey())).flatMap(entry -> entry.getValue().stream())
                .map(AtlasEntity::getTypeName)
                .collect(Collectors.toSet());

        // NOTE: Cascading DELETE will be performed when parent NiFiFlow is updated without removed DataSet entities.
        final Map<String, List<AtlasEntity>> remainingEntitiesByType = changedEntities.entrySet().stream()
                .filter(entry -> !DELETED.equals(entry.getKey()))
                .flatMap(entry -> entry.getValue().stream())
                .filter(entity -> changedTypeNames.contains(entity.getTypeName()))
                .collect(Collectors.groupingBy(AtlasEntity::getTypeName));

        // If all entities are deleted for a type (e.g. nifi_intput_port), then remainingEntitiesByType will not contain such key.
        // If the returning map does not contain anything for a type, then the corresponding attribute will not be updated.
        // To empty an attribute when all of its elements are deleted, add empty list for a type.
        changedTypeNames.forEach(changedTypeName -> remainingEntitiesByType.computeIfAbsent(changedTypeName, k -> Collections.emptyList()));
        return remainingEntitiesByType;
    }

    private Set<AtlasObjectId> registerFlowPathEntities(final NiFiFlow nifiFlow) throws AtlasServiceException {

        final Map<NiFiFlow.EntityChangeType, List<AtlasEntity>> changedEntities = nifiFlow.getChangedFlowPathEntities();

        if (changedEntities.containsKey(CREATED)) {
            final List<AtlasEntity> createdEntities = changedEntities.get(CREATED);
            final AtlasEntity.AtlasEntitiesWithExtInfo atlasEntities = new AtlasEntity.AtlasEntitiesWithExtInfo(createdEntities);
            final EntityMutationResponse mutationResponse = atlasClient.createEntities(atlasEntities);
            logger.debug("Created FlowPath entities mutation response={}", mutationResponse);

            final Map<String, String> guidAssignments = mutationResponse.getGuidAssignments();
            createdEntities.forEach(entity -> {
                final String guid = entity.getGuid();
                entity.setGuid(guidAssignments.get(guid));
                final String pathId = getComponentIdFromQualifiedName(getQualifiedName(entity));
                final NiFiFlowPath path = nifiFlow.getFlowPaths().get(pathId);
                path.setAtlasEntity(entity);
            });
        }

        if (changedEntities.containsKey(UPDATED)) {
            final List<AtlasEntity> updatedEntities = changedEntities.get(UPDATED);
            final AtlasEntity.AtlasEntitiesWithExtInfo atlasEntities = new AtlasEntity.AtlasEntitiesWithExtInfo(updatedEntities);
            final EntityMutationResponse mutationResponse = atlasClient.updateEntities(atlasEntities);
            logger.debug("Updated FlowPath entities mutation response={}", mutationResponse);
            updatedEntities.forEach(entity -> {
                final String pathId = getComponentIdFromQualifiedName(getQualifiedName(entity));
                final NiFiFlowPath path = nifiFlow.getFlowPaths().get(pathId);
                path.setAtlasEntity(entity);
            });
        }

        if (NiFiFlow.EntityChangeType.containsChange(changedEntities.keySet())) {
            return changedEntities.entrySet().stream()
                    .filter(entry -> !DELETED.equals(entry.getKey())).flatMap(entry -> entry.getValue().stream())
                    .map(path -> new AtlasObjectId(path.getGuid(), TYPE_NIFI_FLOW_PATH,
                            Collections.singletonMap(ATTR_QUALIFIED_NAME, getQualifiedName(path))))
                    .collect(Collectors.toSet());
        }

        return null;
    }

    public AtlasEntity.AtlasEntityWithExtInfo searchEntityDef(AtlasObjectId id) throws AtlasServiceException {
        final String guid = id.getGuid();
        if (!StringUtils.isEmpty(guid)) {
            return atlasClient.getEntityByGuid(guid, true, false);
        }
        final Map<String, String> attributes = new HashMap<>();
        id.getUniqueAttributes().entrySet().stream().filter(entry -> entry.getValue() != null)
                .forEach(entry -> attributes.put(entry.getKey(), entry.getValue().toString()));
        return atlasClient.getEntityByAttribute(id.getTypeName(), attributes, true, false);
    }

    public void saveLineage(LineageContext lineageContext) throws AtlasServiceException {
        logger.debug("Saving LineageContext: {}", lineageContext);

        Predicate<AtlasEntityWithExtInfo> isNewEntity = entityExt -> !isGuidAssigned(entityExt.getEntity().getGuid());

        Map<String, AtlasEntity> flowPaths = lineageContext.getFlowPaths();
        List<AtlasEntityWithExtInfo> newFlowPaths = flowPaths.values().stream()
                .map(AtlasEntityWithExtInfo::new)
                .filter(isNewEntity)
                .collect(Collectors.toList());
        createEntities(newFlowPaths);

        Map<String, AtlasEntityWithExtInfo> dataSets = lineageContext.getDataSets();
        List<AtlasEntityWithExtInfo> newDataSets = dataSets.values().stream()
                .filter(isNewEntity)
                .collect(Collectors.toList());
        createEntities(newDataSets);

        createFlowPathDataSetRelationships(flowPaths, dataSets, lineageContext.getFlowPathInputs(), true);
        createFlowPathDataSetRelationships(flowPaths, dataSets, lineageContext.getFlowPathOutputs(), false);

        lineageContext.commit();
    }

    private void createEntities(List<AtlasEntityWithExtInfo> entityExtList) throws AtlasServiceException {
        for (List<AtlasEntityWithExtInfo> entityExtBatchList: ListUtils.partition(entityExtList, CREATE_BATCH_SIZE)) {
            AtlasEntitiesWithExtInfo entitiesExt = new AtlasEntitiesWithExtInfo();

            for (AtlasEntityWithExtInfo entityExt : entityExtBatchList) {
                entitiesExt.addEntity(entityExt.getEntity());
                if (entityExt.getReferredEntities() != null) {
                    for (AtlasEntity referredEntity : entityExt.getReferredEntities().values()) {
                        entitiesExt.addReferredEntity(referredEntity);
                    }
                }
            }

            EntityMutationResponse response = atlasClient.createEntities(entitiesExt);

            for (AtlasEntityWithExtInfo entityExt : entityExtBatchList) {
                AtlasEntity entity = entityExt.getEntity();
                String assignedGuid = response.getGuidAssignments().get(entity.getGuid());
                if (assignedGuid == null) {
                    throw new IllegalStateException("Entity creation failed for entity: " + getTypedQualifiedName(entity));
                }
                entity.setGuid(assignedGuid);
            }
        }
    }

    private void createFlowPathDataSetRelationships(Map<String, AtlasEntity> flowPaths,
                                                    Map<String, AtlasEntityWithExtInfo> dataSets,
                                                    Map<String, Set<String>> flowPathDataSets,
                                                    boolean isInput) throws AtlasServiceException {
        for (Map.Entry<String, Set<String>> entry: flowPathDataSets.entrySet()) {
            String flowPathQualifiedName = entry.getKey();
            String flowPathGuid = flowPaths.get(flowPathQualifiedName).getGuid();

            String relationshipName = isInput ? REL_PROCESS_INPUT : REL_PROCESS_OUTPUT;

            for (String dataSetTypedQualifiedName: entry.getValue()) {
                String dataSetGuid = dataSets.get(dataSetTypedQualifiedName).getEntity().getGuid();

                AtlasRelationship relationship = new AtlasRelationship(relationshipName);

                relationship.setEnd1(new AtlasObjectId(flowPathGuid));
                relationship.setEnd2(new AtlasObjectId(dataSetGuid));

                try {
                    atlasClient.createRelationship(relationship);
                } catch (AtlasServiceException ase) {
                    if (ase.getStatus() == ClientResponse.Status.CONFLICT) {
                        // the relationship has already been created by another node in the cluster
                        logger.info("Relationship already exists. FlowPath: guid={} / qualifiedName={}, DataSet: guid={} / typedQualifiedName={}",
                                flowPathGuid, flowPathQualifiedName, dataSetGuid, dataSetTypedQualifiedName);
                    } else {
                        throw ase;
                    }
                }
            }
        }
    }

}
