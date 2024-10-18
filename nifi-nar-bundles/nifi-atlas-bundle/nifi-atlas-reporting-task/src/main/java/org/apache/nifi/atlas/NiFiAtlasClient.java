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
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.SearchFilter;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasRelationship;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.commons.collections4.ListUtils;
import org.apache.nifi.atlas.NiFiTypes.RelationshipType;
import org.apache.nifi.atlas.model.NiFiAtlasEntity;
import org.apache.nifi.atlas.model.NiFiComponent;
import org.apache.nifi.atlas.model.NiFiFlow;
import org.apache.nifi.atlas.model.NiFiFlowPath;
import org.apache.nifi.atlas.model.NiFiInputPort;
import org.apache.nifi.atlas.model.NiFiOutputPort;
import org.apache.nifi.atlas.model.NiFiQueue;
import org.apache.nifi.atlas.model.RelationshipInfo;
import org.apache.nifi.atlas.provenance.DataSet;
import org.apache.nifi.atlas.provenance.lineage.LineageContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.MultivaluedMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.nifi.atlas.AtlasUtils.isGuidAssigned;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_URL;
import static org.apache.nifi.atlas.NiFiTypes.ENTITIES;
import static org.apache.nifi.atlas.NiFiTypes.REL_TYPE_PROCESS_INPUT;
import static org.apache.nifi.atlas.NiFiTypes.REL_TYPE_PROCESS_OUTPUT;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_FLOW;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_FLOW_PATH;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_INPUT_PORT;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_OUTPUT_PORT;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_QUEUE;

public class NiFiAtlasClient implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(NiFiAtlasClient.class);

    private static final int FETCH_BATCH_SIZE = 100;
    private static final int SAVE_BATCH_SIZE = 100;

    private final AtlasClientV2 atlasClient;

    private final LineageCache lineageCache;

    public NiFiAtlasClient(AtlasClientV2 atlasClient, LineageCache lineageCache) {
        this.atlasClient = atlasClient;
        this.lineageCache = lineageCache;
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

    /**
     * Fetch existing NiFiFlow entity from Atlas.
     * @param rootProcessGroupId The id of a NiFi flow root process group.
     * @param namespace The namespace of a flow.
     * @return A NiFiFlow instance filled with retrieved data from Atlas. Status objects are left blank, e.g. ProcessorStatus.
     * @throws AtlasServiceException Thrown if requesting to Atlas API failed, including when the flow is not found.
     */
    public NiFiFlow fetchNiFiFlow(String rootProcessGroupId, String namespace) throws AtlasServiceException {
        final String qualifiedName = AtlasUtils.toQualifiedName(namespace, rootProcessGroupId);
        final AtlasEntity.AtlasEntityWithExtInfo nifiFlowExt = atlasClient.getEntityByAttribute(TYPE_NIFI_FLOW, Collections.singletonMap(ATTR_QUALIFIED_NAME, qualifiedName), true, true);

        final AtlasEntity nifiFlowEntity = nifiFlowExt.getEntity();
        final Map<String, AtlasEntity> referredEntities = nifiFlowExt.getReferredEntities();
        final NiFiFlow nifiFlow = new NiFiFlow(nifiFlowEntity, namespace);

        fetchFlowPaths(referredEntities).forEach(e -> nifiFlow.addFlowPath(new NiFiFlowPath(e)));
        fetchQueues(referredEntities).forEach(e -> nifiFlow.addQueue(new NiFiQueue(e)));

        getFilteredReferredEntities(TYPE_NIFI_INPUT_PORT, referredEntities).forEach(e -> nifiFlow.addInputPort(new NiFiInputPort(e)));
        getFilteredReferredEntities(TYPE_NIFI_OUTPUT_PORT, referredEntities).forEach(e -> nifiFlow.addOutputPort(new NiFiOutputPort(e)));

        nifiFlow.connectFlowPathsAndQueues();

        return nifiFlow;
    }

    /**
     * Retrieves FlowPaths metadata without fetching all DataSets connected to the FlowPaths.
     *
     * @param referredEntities referred entities of the flow entity (returned when the flow fetched) containing the basic data (id, status) of the flow components
     * @return FlowPath referred entities updated with the fetched metadata
     */
    private List<AtlasEntity> fetchFlowPaths(Map<String, AtlasEntity> referredEntities) {
        final List<String> guids = getFilteredReferredEntitiesGuids(TYPE_NIFI_FLOW_PATH, referredEntities);
        final List<AtlasEntity> flowPaths = new ArrayList<>(guids.size());
        for (List<String> batch : ListUtils.partition(guids, FETCH_BATCH_SIZE)) {
            final StringBuilder query = new StringBuilder();
            query.append("from nifi_flow_path where __guid=[");
            for (int i = 0; i < batch.size(); i++) {
                String guid = batch.get(i);
                if (i > 0) {
                    query.append(", ");
                }
                query.append('"');
                query.append(guid);
                query.append('"');
            }
            query.append("] select __guid, url");

            try {
                final AtlasSearchResult result = atlasClient.dslSearch(query.toString());
                List<List<Object>> table = result.getAttributes().getValues();
                for (List<Object> row : table) {
                    final String guid = (String) row.get(0);
                    final String url = (String) row.get(1);
                    final AtlasEntity flowPath = referredEntities.get(guid);
                    flowPath.setAttribute(ATTR_URL, url);
                    flowPaths.add(flowPath);
                }
            } catch (AtlasServiceException e) {
                throw new ProcessException(String.format("Failed to search entities by guids %s", batch), e);
            }
        }
        return flowPaths;
    }

    /**
     * Retrieves Queues with all connected FlowPaths.
     *
     * @param referredEntities referred entities of the flow entity (returned when the flow fetched) containing the basic data (id, status) of the flow components
     * @return Queue entities containing relationship data
     */
    private List<AtlasEntity> fetchQueues(Map<String, AtlasEntity> referredEntities) {
        final List<String> guids = getFilteredReferredEntitiesGuids(TYPE_NIFI_QUEUE, referredEntities);
        final List<AtlasEntity> queues = new ArrayList<>(guids.size());
        for (List<String> batch : ListUtils.partition(guids, FETCH_BATCH_SIZE)) {
            try {
                AtlasEntitiesWithExtInfo result = atlasClient.getEntitiesByGuids(batch);
                queues.addAll(result.getEntities());
            } catch (AtlasServiceException e) {
                throw new ProcessException(String.format("Failed to search entities by guids %s", batch), e);
            }
        }
        return queues;
    }

    private List<String> getFilteredReferredEntitiesGuids(String componentType, Map<String, AtlasEntity> referredEntities) {
        return getFilteredReferredEntities(componentType, referredEntities)
                .map(AtlasEntity::getGuid)
                .collect(Collectors.toList());
    }

    private Stream<AtlasEntity> getFilteredReferredEntities(String componentType, Map<String, AtlasEntity> referredEntities) {
        return referredEntities.values().stream()
                .filter(referredEntity -> referredEntity.getTypeName().equals(componentType))
                .filter(referredEntity -> referredEntity.getStatus() == AtlasEntity.Status.ACTIVE);
    }

    public void registerNiFiFlow(NiFiFlow nifiFlow) throws AtlasServiceException {
        if (nifiFlow.isCreated()) {
            // save the new flow to retrieve the assigned guid
            createNiFiEntity(nifiFlow);
        }

        // create / update the flow components, retrieve the assigned guid for created ones
        registerFlowComponents(nifiFlow);

        // create / delete FlowPath - Queue relationships
        registerFlowPathQueueRelationships(nifiFlow.getFlowPaths());

        // update NiFiFlow with changed flow component lists (if any)
        nifiFlow.updateFlowComponentReferences();

        // save the updated NiFiFlow (metadata and/or referenced flow components)
        if (nifiFlow.isCreated() || nifiFlow.isUpdated()) {
            updateNiFiEntity(nifiFlow);
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
    }

    private void registerFlowComponents(NiFiFlow nifiFlow) throws AtlasServiceException {
        final AtlasObjectId nifiFlowObjectId = new AtlasObjectId(nifiFlow.getGuid());

        registerFlowComponents(nifiFlowObjectId, nifiFlow.getFlowPaths());
        registerFlowComponents(nifiFlowObjectId, nifiFlow.getQueues());
        registerFlowComponents(nifiFlowObjectId, nifiFlow.getInputPorts());
        registerFlowComponents(nifiFlowObjectId, nifiFlow.getOutputPorts());
    }

    private void registerFlowComponents(AtlasObjectId nifiFlowObjectId, final Map<String, ? extends NiFiComponent> flowComponents) throws AtlasServiceException {
        final List<? extends NiFiComponent> createdFlowComponents = flowComponents.values().stream()
                .filter(NiFiAtlasEntity::isCreated)
                .peek(fc -> fc.setNiFiFlow(nifiFlowObjectId))
                .collect(Collectors.toList());

        createNiFiEntities(createdFlowComponents);

        final List<? extends NiFiComponent> updatedFlowComponents = flowComponents.values().stream()
                .filter(NiFiAtlasEntity::isUpdated)
                .collect(Collectors.toList());

        updateNiFiEntities(updatedFlowComponents);
    }

    private void createNiFiEntity(final NiFiAtlasEntity nifiEntity)  throws AtlasServiceException {
        createNiFiEntities(Collections.singletonList(nifiEntity));
    }

    private void createNiFiEntities(final List<? extends NiFiAtlasEntity> nifiEntities)  throws AtlasServiceException {
        for (List<? extends NiFiAtlasEntity> batch : ListUtils.partition(nifiEntities, SAVE_BATCH_SIZE)) {
            final AtlasEntitiesWithExtInfo atlasEntitiesExt = new AtlasEntitiesWithExtInfo();
            batch.forEach(ne -> atlasEntitiesExt.addEntity(ne.getAtlasEntity()));

            final EntityMutationResponse response = atlasClient.createEntities(atlasEntitiesExt);

            final Map<String, String> guidAssignments = response.getGuidAssignments();
            batch.forEach(ne -> ne.setGuid(guidAssignments.get(ne.getGuid())));
        }
    }

    private void updateNiFiEntity(final NiFiAtlasEntity nifiEntity)  throws AtlasServiceException {
        updateNiFiEntities(Collections.singletonList(nifiEntity));
    }

    private void updateNiFiEntities(final List<? extends NiFiAtlasEntity> nifiEntities)  throws AtlasServiceException {
        for (List<? extends NiFiAtlasEntity> batch : ListUtils.partition(nifiEntities, SAVE_BATCH_SIZE)) {
            final AtlasEntitiesWithExtInfo atlasEntitiesExt = new AtlasEntitiesWithExtInfo();
            batch.forEach(ne -> atlasEntitiesExt.addEntity(ne.getAtlasEntity()));

            atlasClient.updateEntities(atlasEntitiesExt);
        }
    }

    private void registerFlowPathQueueRelationships(final Map<String, NiFiFlowPath> flowPaths) throws AtlasServiceException {
        for (NiFiFlowPath flowPath : flowPaths.values()) {
            final AtlasObjectId flowPathObjectId = new AtlasObjectId(flowPath.getGuid());
            registerFlowPathQueueRelationships(flowPathObjectId, flowPath.getInputQueues(), REL_TYPE_PROCESS_INPUT);
            registerFlowPathQueueRelationships(flowPathObjectId, flowPath.getOutputQueues(), REL_TYPE_PROCESS_OUTPUT);
        }
    }

    private void registerFlowPathQueueRelationships(final AtlasObjectId flowPathObjectId,
                                                    final Map<NiFiQueue, RelationshipInfo> queues,
                                                    final String relationshipTypeName) throws AtlasServiceException {
        for (Map.Entry<NiFiQueue, RelationshipInfo> entry : queues.entrySet()) {
            final NiFiQueue queue = entry.getKey();
            final RelationshipInfo relationshipInfo = entry.getValue();

            if (relationshipInfo.isCreated()) {
                final AtlasRelationship relationship = new AtlasRelationship(relationshipTypeName);
                relationship.setEnd1(flowPathObjectId);
                relationship.setEnd2(new AtlasObjectId(queue.getGuid()));
                try {
                    atlasClient.createRelationship(relationship);
                } catch (AtlasServiceException ase) {
                    if (ase.getStatus() == ClientResponse.Status.CONFLICT) {
                        logger.warn("Relationship already exists. FlowPath.guid={}, Queue.guid={}", flowPathObjectId.getGuid(), queue.getGuid());
                    } else {
                        throw ase;
                    }
                }
            } else if (!relationshipInfo.isActive()) {
                try {
                    atlasClient.deleteRelationshipByGuid(relationshipInfo.getGuid());
                } catch (AtlasServiceException ase) {
                    if (ase.getStatus() == ClientResponse.Status.NOT_FOUND) {
                        logger.warn("Relationship already deleted. Relationship.guid={}, FlowPath.guid={}, Queue: guid={}", relationshipInfo.getGuid(), flowPathObjectId.getGuid(), queue.getGuid());
                    } else {
                        throw ase;
                    }
                }
            }
        }
    }

    public void saveLineage(LineageContext lineageContext) throws AtlasServiceException {
        logger.debug("Saving LineageContext: {}", lineageContext);

        Map<String, NiFiFlowPath> flowPaths = lineageContext.getFlowPaths();
        List<NiFiFlowPath> newFlowPaths = flowPaths.values().stream()
                .filter(fp -> !isGuidAssigned(fp.getGuid()))
                .collect(Collectors.toList());
        createNiFiEntities(newFlowPaths);

        Map<String, DataSet> dataSets = lineageContext.getDataSets();
        List<DataSet> newDataSets = dataSets.values().stream()
                .filter(DataSet::isGuidNotAssigned)
                .peek(ds -> lineageCache.getDataSetGuid(ds.getTypedQualifiedName()).ifPresent(ds::setGuid))
                .filter(DataSet::isGuidNotAssigned)
                .collect(Collectors.toList());
        createDataSetEntities(newDataSets);

        newDataSets.forEach(ds -> lineageCache.addDataSetGuid(ds.getTypedQualifiedName(), ds.getGuid()));

        createFlowPathDataSetRelationships(flowPaths, dataSets, lineageContext.getFlowPathInputs(), RelationshipType.PROCESS_INPUT);
        createFlowPathDataSetRelationships(flowPaths, dataSets, lineageContext.getFlowPathOutputs(), RelationshipType.PROCESS_OUTPUT);
    }

    private void createDataSetEntities(List<DataSet> dataSets) throws AtlasServiceException {
        for (List<DataSet> dataSetsBatch: ListUtils.partition(dataSets, SAVE_BATCH_SIZE)) {
            AtlasEntitiesWithExtInfo entitiesExt = new AtlasEntitiesWithExtInfo();

            for (DataSet dataSet : dataSetsBatch) {
                entitiesExt.addEntity(dataSet.getEntity());
                for (AtlasEntity referredEntity : dataSet.getReferredEntities()) {
                    entitiesExt.addReferredEntity(referredEntity);
                }
            }

            EntityMutationResponse response = atlasClient.createEntities(entitiesExt);

            final Map<String, String> guidAssignments = response.getGuidAssignments();
            dataSetsBatch.forEach(ds -> ds.setGuid(guidAssignments.get(ds.getGuid())));
        }
    }

    private void createFlowPathDataSetRelationships(Map<String, NiFiFlowPath> flowPaths,
                                                    Map<String, DataSet> dataSets,
                                                    Map<String, Set<String>> flowPathDataSets,
                                                    RelationshipType relationshipType) throws AtlasServiceException {
        for (Map.Entry<String, Set<String>> entry: flowPathDataSets.entrySet()) {
            String flowPathQualifiedName = entry.getKey();
            String flowPathGuid = flowPaths.get(flowPathQualifiedName).getGuid();

            for (String dataSetTypedQualifiedName: entry.getValue()) {
                String dataSetGuid = dataSets.get(dataSetTypedQualifiedName).getGuid();

                if (lineageCache.containsRelationship(relationshipType, flowPathGuid, dataSetGuid)) {
                    // TODO: debug log
                    continue;
                }

                AtlasRelationship relationship = new AtlasRelationship(relationshipType.getName());

                relationship.setEnd1(new AtlasObjectId(flowPathGuid));
                relationship.setEnd2(new AtlasObjectId(dataSetGuid));

                try {
                    atlasClient.createRelationship(relationship);
                } catch (AtlasServiceException ase) {
                    if (ase.getStatus() == ClientResponse.Status.CONFLICT) {
                        // the relationship has already been created by another node in the cluster or during earlier execution of the reporting task
                        logger.info("Relationship already exists. FlowPath: guid={} / qualifiedName={}, DataSet: guid={} / typedQualifiedName={}",
                                flowPathGuid, flowPathQualifiedName, dataSetGuid, dataSetTypedQualifiedName);
                    } else {
                        throw ase;
                    }
                }

                lineageCache.addRelationship(relationshipType, flowPathGuid, dataSetGuid);
            }
        }
    }

}
