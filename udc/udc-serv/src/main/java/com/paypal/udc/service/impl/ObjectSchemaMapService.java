/*
 * Copyright 2019 PayPal Inc.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.paypal.udc.service.impl;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import javax.validation.ConstraintViolationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.paypal.udc.dao.cluster.ClusterRepository;
import com.paypal.udc.dao.dataset.DatasetChangeLogRepository;
import com.paypal.udc.dao.dataset.DatasetRepository;
import com.paypal.udc.dao.dataset.DatasetStorageSystemRepository;
import com.paypal.udc.dao.objectschema.ObjectSchemaAttributeCustomKeyValueRepository;
import com.paypal.udc.dao.objectschema.ObjectSchemaAttributeValueRepository;
import com.paypal.udc.dao.objectschema.ObjectSchemaMapRepository;
import com.paypal.udc.dao.objectschema.PageableObjectSchemaMapRepository;
import com.paypal.udc.dao.storagesystem.StorageSystemRepository;
import com.paypal.udc.dao.storagetype.StorageTypeAttributeKeyRepository;
import com.paypal.udc.entity.cluster.Cluster;
import com.paypal.udc.entity.dataset.Dataset;
import com.paypal.udc.entity.dataset.DatasetChangeLog;
import com.paypal.udc.entity.dataset.DatasetStorageSystem;
import com.paypal.udc.entity.objectschema.CollectiveObjectAttributeValue;
import com.paypal.udc.entity.objectschema.CollectiveObjectSchemaMap;
import com.paypal.udc.entity.objectschema.ObjectAttributeKeyValue;
import com.paypal.udc.entity.objectschema.ObjectAttributeValue;
import com.paypal.udc.entity.objectschema.ObjectSchemaMap;
import com.paypal.udc.entity.objectschema.Schema;
import com.paypal.udc.entity.storagesystem.StorageSystem;
import com.paypal.udc.entity.storagetype.StorageType;
import com.paypal.udc.entity.storagetype.StorageTypeAttributeKey;
import com.paypal.udc.exception.ValidationError;
import com.paypal.udc.service.IObjectSchemaMapService;
import com.paypal.udc.util.ClusterUtil;
import com.paypal.udc.util.DatasetUtil;
import com.paypal.udc.util.ObjectSchemaMapUtil;
import com.paypal.udc.util.StorageSystemUtil;
import com.paypal.udc.util.StorageTypeUtil;
import com.paypal.udc.util.UserUtil;
import com.paypal.udc.util.enumeration.ActiveEnumeration;
import com.paypal.udc.util.enumeration.TimelineEnumeration;


@Service
@Transactional(propagation = Propagation.SUPPORTS, readOnly = true)
public class ObjectSchemaMapService implements IObjectSchemaMapService {

    @Autowired
    private ObjectSchemaMapRepository schemaMapRepository;
    @Autowired
    private ObjectSchemaAttributeValueRepository objectAttributeRepository;
    @Autowired
    private StorageSystemUtil storageSystemUtil;
    @Autowired
    private StorageTypeUtil storageTypeUtil;
    @Autowired
    private ObjectSchemaMapUtil schemaMapUtil;
    @Autowired
    private StorageSystemRepository storageSystemRepository;
    @Autowired
    private ClusterRepository clusterRepository;
    @Autowired
    private DatasetStorageSystemRepository datasetSystemRepository;
    @Autowired
    private DatasetRepository datasetRepository;
    @Autowired
    private UserUtil userUtil;
    @Autowired
    private ClusterUtil clusterUtil;
    @Autowired
    private DatasetUtil datasetUtil;
    @Autowired
    private PageableObjectSchemaMapRepository pageableSchemaMapRepository;
    @Autowired
    private StorageTypeAttributeKeyRepository stakr;
    @Autowired
    private ObjectSchemaAttributeCustomKeyValueRepository sackvr;
    @Autowired
    private DatasetChangeLogRepository changeLogRepository;
    @Value("${elasticsearch.dataset.name}")
    private String esDatasetIndex;
    @Value("${elasticsearch.type.name}")
    private String esType;
    @Value("${udc.es.write.enabled}")
    private String isEsWriteEnabled;
    @Autowired
    private ElasticsearchTemplate esTemplate;
    @Autowired
    private DatasetUtil dataSetUtil;

    final static Logger logger = LoggerFactory.getLogger(ObjectSchemaMapService.class);
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss");

    private final String unknownString = "Unknown";

    @Override
    public Page<CollectiveObjectSchemaMap> getPagedUnRegisteredObjects(final long systemId, final Pageable pageable) {
        final Gson gson = new Gson();

        final Page<ObjectSchemaMap> pagedObjectSchemaMap = this.pageableSchemaMapRepository
                .findByStorageSystemIdAndIsActiveYNAndIsRegisteredAndIsSelfDiscovered(systemId,
                        ActiveEnumeration.YES.getFlag(), ActiveEnumeration.NO.getFlag(),
                        ActiveEnumeration.NO.getFlag(), pageable);
        final List<Long> clusters = this.clusterUtil.getAllClusters().stream()
                .map(cluster -> cluster.getClusterId())
                .collect(Collectors.toList());
        final List<ObjectSchemaMap> objects = pagedObjectSchemaMap.getContent();

        final List<CollectiveObjectSchemaMap> modifiedObjects = this.schemaMapUtil.getPagedObjectSchemaMaps(objects,
                gson, this.schemaMapRepository, pageable,
                this.objectAttributeRepository, systemId, clusters);

        final long totalCount = this.schemaMapRepository.countByStorageSystemIdAndIsActiveYNAndIsRegistered(systemId,
                ActiveEnumeration.YES.getFlag(), ActiveEnumeration.NO.getFlag());
        final Page<CollectiveObjectSchemaMap> pages = new PageImpl<CollectiveObjectSchemaMap>(
                modifiedObjects, pageable, totalCount);
        return pages;

    }

    @Override
    public Page<CollectiveObjectSchemaMap> getPagedObjectMappings(final long storageSystemId, final Pageable pageable) {
        final Gson gson = new Gson();

        final Page<ObjectSchemaMap> pagedObjectSchemaMap = this.pageableSchemaMapRepository
                .findByStorageSystemIdAndIsSelfDiscovered(storageSystemId, ActiveEnumeration.NO.getFlag(), pageable);
        final List<Long> clusters = this.clusterUtil.getAllClusters().stream()
                .map(cluster -> cluster.getClusterId())
                .collect(Collectors.toList());
        final List<ObjectSchemaMap> objects = pagedObjectSchemaMap.getContent();
        final List<CollectiveObjectSchemaMap> modifiedObjects = this.schemaMapUtil.getPagedObjectSchemaMaps(objects,
                gson, this.schemaMapRepository, pageable,
                this.objectAttributeRepository, storageSystemId, clusters);

        final long totalCount = this.schemaMapRepository.countByStorageSystemId(storageSystemId);
        final Page<CollectiveObjectSchemaMap> pages = new PageImpl<CollectiveObjectSchemaMap>(
                modifiedObjects, pageable, totalCount);
        return pages;

    }

    @Override
    public List<ObjectSchemaMap> getObjectSchemaMapsBySystemIds(final long storageSystemId) {
        final List<ObjectSchemaMap> schemaMaps = this.schemaMapRepository.findByStorageSystemId(storageSystemId);
        return schemaMaps;
    }

    @Override
    public ObjectSchemaMap getDatasetById(final long topicId) throws ValidationError {

        final Gson gson = new Gson();
        final Map<Long, StorageTypeAttributeKey> storageTypeAttributeMap = this.storageTypeUtil
                .getStorageTypeAttributes();
        final ObjectSchemaMap topic = this.schemaMapUtil.validateObjectSchemaMapId(topicId);
        final StorageType storageType = this.schemaMapUtil.getTypeFromObject(topic);
        final List<Long> clusters = this.clusterUtil.getAllClusters().stream().map(cluster -> cluster.getClusterId())
                .collect(Collectors.toList());
        topic.setClusters(clusters);
        final List<ObjectAttributeValue> objectAttributes = this.objectAttributeRepository
                .findByObjectIdAndIsActiveYN(topicId, ActiveEnumeration.YES.getFlag());
        objectAttributes.forEach(attr -> {
            final StorageTypeAttributeKey typeAttrKey = storageTypeAttributeMap.get(attr.getStorageDsAttributeKeyId());
            attr.setStorageDsAttributeKeyName(typeAttrKey.getStorageDsAttributeKeyName());
        });

        final List<StorageTypeAttributeKey> storageTypeAttributesList = this.stakr
                .findByStorageTypeIdAndIsStorageSystemLevelAndIsActiveYN(storageType.getStorageTypeId(),
                        ActiveEnumeration.NO.getFlag(), ActiveEnumeration.YES.getFlag());

        final String objectSchemaInString = topic.getObjectSchemaInString();
        if (objectSchemaInString != null && objectSchemaInString.length() > 0) {
            final List<Schema> objectSchema = gson.fromJson(objectSchemaInString, new TypeToken<List<Schema>>() {
            }.getType());
            topic.setObjectSchema(objectSchema);
        }
        else {
            topic.setObjectSchema(new ArrayList<Schema>());
        }

        if (storageTypeAttributesList.size() != objectAttributes.size()) {

            final List<Long> objectAttributeIds = objectAttributes.stream()
                    .map(attribute -> attribute.getStorageDsAttributeKeyId()).collect(Collectors.toList());

            final Map<Long, StorageTypeAttributeKey> typeAttributesMap = storageTypeAttributesList.stream()
                    .collect(Collectors.toMap(p -> p.getStorageDsAttributeKeyId(), p -> p));

            final List<Long> typeAttributeIds = storageTypeAttributesList.stream()
                    .map(attribute -> attribute.getStorageDsAttributeKeyId()).collect(Collectors.toList()).stream()
                    .filter(attribute -> !objectAttributeIds.contains(attribute)).collect(Collectors.toList());
            typeAttributeIds.forEach(attribute -> {
                final StorageTypeAttributeKey typeAttribute = typeAttributesMap.get(attribute);

                final ObjectAttributeValue pendingAttribute = new ObjectAttributeValue();
                pendingAttribute.setObjectAttributeValueId(0);
                pendingAttribute.setObjectId(topicId);
                pendingAttribute.setStorageDsAttributeKeyId(attribute);
                pendingAttribute.setObjectAttributeValue("Defaults");
                pendingAttribute.setIsCustomized(ActiveEnumeration.NO.getFlag());
                pendingAttribute.setIsActiveYN(ActiveEnumeration.YES.getFlag());
                pendingAttribute.setStorageDsAttributeKeyName(typeAttribute.getStorageDsAttributeKeyName());
                objectAttributes.add(pendingAttribute);
            });
        }
        topic.setObjectAttributes(objectAttributes);
        return topic;
    }

    @Override
    public ObjectAttributeKeyValue updateObjectAttributeKeyValue(final ObjectAttributeKeyValue topic)
            throws ValidationError, IOException, InterruptedException, ExecutionException {

        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final ValidationError v = new ValidationError();
        final String time = sdf.format(timestamp);
        final String user = topic.getUpdatedUser() == null ? "udc_admin" : topic.getUpdatedUser();
        ObjectAttributeKeyValue updatedAttributeKeyValue;
        try {
            final ObjectAttributeKeyValue attributeKeyValue = this.sackvr
                    .findById(topic.getObjectAttributeKeyValueId()).orElse(null);
            final ObjectSchemaMap osm = this.schemaMapUtil.validateObjectSchemaMapId(topic.getObjectId());
            this.schemaMapUtil.validateObjectAttributeKey(topic.getObjectAttributeKey(), osm);

            if (attributeKeyValue == null) {
                v.setErrorCode(HttpStatus.BAD_REQUEST);
                v.setErrorDescription("Object Attribute Key value is invalid");
                throw v;
            }
            attributeKeyValue.setUpdatedTimestamp(time);
            attributeKeyValue.setUpdatedUser(user);
            attributeKeyValue.setObjectAttributeKey(topic.getObjectAttributeKey());
            attributeKeyValue.setObjectAttributeValue(topic.getObjectAttributeValue());
            updatedAttributeKeyValue = this.sackvr.save(attributeKeyValue);

        }
        catch (final ConstraintViolationException e) {
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("Object Attribute Key is empty");
            throw v;
        }

        if (this.isEsWriteEnabled.equals("true")) {
            final List<Dataset> datasets = this.datasetRepository.findByObjectSchemaMapId(topic.getObjectId());
            if (datasets != null && datasets.size() > 0) {
                for (final Dataset dataset : datasets) {
                    this.dataSetUtil.upsertDataset(this.esDatasetIndex, this.esType, dataset, this.esTemplate);
                }

            }
        }
        return updatedAttributeKeyValue;
    }

    @Override
    public ObjectAttributeKeyValue addObjectAttributeKeyValue(final ObjectAttributeKeyValue topic)
            throws ValidationError, IOException, InterruptedException, ExecutionException {

        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final ValidationError v = new ValidationError();
        final String time = sdf.format(timestamp);
        final String user = topic.getCreatedUser() == null ? "udc_admin" : topic.getCreatedUser();
        ObjectAttributeKeyValue insertedTopic;
        try {
            final ObjectSchemaMap osm = this.schemaMapUtil.validateObjectSchemaMapId(topic.getObjectId());
            this.schemaMapUtil.validateObjectAttributeKey(topic.getObjectAttributeKey(), osm);
            // this.userUtil.validateUser(user);
            topic.setCreatedTimestamp(time);
            topic.setUpdatedTimestamp(time);
            topic.setCreatedUser(user);
            topic.setUpdatedUser(user);
            topic.setIsActiveYN(ActiveEnumeration.YES.getFlag());
            insertedTopic = this.sackvr.save(topic);
        }
        catch (final ConstraintViolationException e) {
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("Object Attribute Key is empty");
            throw v;
        }
        catch (final DataIntegrityViolationException e) {
            v.setErrorCode(HttpStatus.CONFLICT);
            v.setErrorDescription("Object Attribute Key and Object ID are duplicated");
            throw v;
        }

        if (this.isEsWriteEnabled.equals("true")) {
            final List<Dataset> datasets = this.datasetRepository.findByObjectSchemaMapId(topic.getObjectId());
            if (datasets != null && datasets.size() > 0) {
                for (final Dataset dataset : datasets) {
                    this.dataSetUtil.upsertDataset(this.esDatasetIndex, this.esType, dataset, this.esTemplate);
                }

            }
        }
        return insertedTopic;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED, readOnly = false, rollbackFor = { ValidationError.class,
            ConstraintViolationException.class, DataIntegrityViolationException.class, ValidationError.class,
            IOException.class, InterruptedException.class, ExecutionException.class })
    public ObjectSchemaMap addObjectSchema(final ObjectSchemaMap schemaMap)
            throws ValidationError, IOException, InterruptedException, ExecutionException {
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final ValidationError v = new ValidationError();
        final Gson gson = new Gson();
        final String time = sdf.format(timestamp);
        final String user = schemaMap.getCreatedUser() == null ? "udc_admin" : schemaMap.getCreatedUser();
        final long storageSystemId = schemaMap.getStorageSystemId();
        final List<Long> clusters = schemaMap.getClusters();
        final Map<Long, Cluster> clusterMap = new HashMap<Long, Cluster>();
        this.clusterRepository.findAll().forEach(cluster -> {
            clusterMap.put(cluster.getClusterId(), cluster);
        });
        StorageType storageType = null;
        String storageTypeName = "";
        try {
            // this.userUtil.validateUser(user);
            // validate Storage System ID
            this.storageSystemUtil.validateStorageSystem(storageSystemId);
            // validate clusters
            this.clusterUtil.validateClusters(clusters);

            // get the specific cluster ID for storage type related to Hive or HBase
            storageType = this.storageSystemUtil.getStorageType(storageSystemId);
            storageTypeName = storageType.getStorageTypeName();
            if (storageTypeName.equalsIgnoreCase("Hbase")) {
                final StorageSystem storageSystem = this.storageSystemUtil.validateStorageSystem(storageSystemId);
                final String storageSystemName = storageSystem.getStorageSystemName();
                clusterMap.forEach((clusterId, cluster) -> {
                    if (storageSystemName.toLowerCase().contains(cluster.getClusterName().toLowerCase())) {
                        final List<Long> tempClusters = new ArrayList<Long>();
                        tempClusters.add(clusterId);
                        schemaMap.setClusters(tempClusters);
                    }
                });
            }

            // populate the schema map object with additional parameters
            schemaMap.setIsSelfDiscovered(schemaMap.getIsSelfDiscovered() == null
                    || schemaMap.getIsSelfDiscovered().equals(ActiveEnumeration.NO.getFlag())
                            ? ActiveEnumeration.NO.getFlag()
                            : schemaMap.getIsSelfDiscovered());
            schemaMap.setCreatedTimestamp(time);
            schemaMap.setUpdatedTimestamp(time);
            schemaMap.setCreatedUser(user);
            schemaMap.setUpdatedUser(user);
            if (schemaMap.getIsSelfDiscovered().equals(ActiveEnumeration.YES.getFlag())) {
                schemaMap.setCreatedTimestampOnStore(this.unknownString);
                schemaMap.setCreatedUserOnStore(this.unknownString);
            }
            schemaMap.setIsRegistered(ActiveEnumeration.NO.getFlag());
            schemaMap.setIsActiveYN(ActiveEnumeration.YES.getFlag());

            final List<Schema> objectSchema = schemaMap.getObjectSchema();
            if (objectSchema != null) {
                final String objectSchemaInString = gson.toJson(objectSchema);
                schemaMap.setObjectSchemaInString(objectSchemaInString);
            }
            final String query = schemaMap.getQuery();
            if (query == null) {
                schemaMap.setQuery("");
            }
            // save the schemaMap object to DatasetSchemaMap table
            this.schemaMapRepository.save(schemaMap);
        }
        catch (final ConstraintViolationException e) {
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("Object name is empty");
            throw v;
        }
        catch (final DataIntegrityViolationException e) {
            v.setErrorCode(HttpStatus.CONFLICT);
            v.setErrorDescription("Object name, Container name and Storage System ID are duplicated");
            throw v;
        }

        // insert into pc_storage_object_attribute_value table
        final List<ObjectAttributeValue> attributeValues = schemaMap.getObjectAttributes();
        if (attributeValues == null || attributeValues.size() == 0) {
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("Object should be uploaded with attributes");
            throw v;
        }
        try {
            attributeValues.forEach(attributeValue -> {
                attributeValue.setObjectId(schemaMap.getObjectId());
                attributeValue.setCreatedUser(user);
                attributeValue.setUpdatedUser(user);
                attributeValue.setCreatedTimestamp(time);
                attributeValue.setUpdatedTimestamp(time);
                attributeValue.setIsCustomized(attributeValue.getIsCustomized() == null ? ActiveEnumeration.NO.getFlag()
                        : attributeValue.getIsCustomized());
                attributeValue.setIsActiveYN(ActiveEnumeration.YES.getFlag());
            });
            this.objectAttributeRepository.saveAll(attributeValues);
        }
        catch (final ConstraintViolationException e) {
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("Object Attribute Value cannot be empty");
            throw v;
        }
        catch (final DataIntegrityViolationException e) {
            v.setErrorCode(HttpStatus.CONFLICT);
            v.setErrorDescription("Storage Type Attribute Key ID is invalid");
            throw v;
        }

        if (this.isEsWriteEnabled.equals("true")) {
            final List<Dataset> datasets = this.datasetRepository.findByObjectSchemaMapId(schemaMap.getObjectId());
            if (datasets != null && datasets.size() > 0) {
                for (final Dataset dataset : datasets) {
                    this.dataSetUtil.upsertDataset(this.esDatasetIndex, this.esType, dataset, this.esTemplate);
                }

            }
        }
        return schemaMap;
    }

    @Override
    public List<String> getDistinctContainerNamesByStorageSystemId(final long storageSystemId) {
        final List<String> containerNames = this.schemaMapRepository
                .findAllContainerNamesByStorageSystemId(storageSystemId);
        return containerNames;
    }

    @Override
    public List<String> getDistinctContainerNames() {
        final List<String> containerNames = this.schemaMapRepository.findAllContainerNames();
        return containerNames;
    }

    @Override
    public List<String> getDistinctContainerNamesBySystems(final String systemList) {
        if (systemList.equals("All")) {
            final List<String> containerNames = this.schemaMapRepository.findAllContainerNames();
            return containerNames;
        }
        else {
            final List<Long> systemIds = this.storageSystemRepository
                    .findByStorageSystemNameIn(Arrays.asList(systemList.split(","))).stream()
                    .map(system -> system.getStorageSystemId()).collect(Collectors.toList());

            if (systemIds != null && systemIds.size() > 0) {
                final List<String> containerNames = this.schemaMapRepository
                        .findAllContainerNamesBySystemIdIn(systemIds);
                return containerNames;
            }
            else {
                return new ArrayList<String>();
            }

        }
    }

    @Override
    public List<String> getDistinctObjectNames(final String containerName, final long storageSystemId) {
        final List<String> objectNames = this.schemaMapRepository.findAllObjectNames(containerName, storageSystemId);
        return objectNames;
    }

    @Override
    public List<Dataset> getDatasetBySystemContainerAndObject(final String systemName, final String containerName,
            final String objectName) throws ValidationError {

        final StorageSystem storageSystem = this.storageSystemUtil.getStorageSystem(systemName);
        final ObjectSchemaMap objectSchemaMap = this.schemaMapRepository
                .findByStorageSystemIdAndContainerNameAndObjectName(
                        storageSystem.getStorageSystemId(),
                        containerName, objectName);

        if (objectSchemaMap != null) {
            final List<Dataset> datasets = this.datasetRepository
                    .findByObjectSchemaMapId(objectSchemaMap.getObjectId());
            return datasets;
        }
        else {
            return new ArrayList<Dataset>();
        }
    }

    @Override
    public Page<ObjectSchemaMap> getObjectsByStorageSystemAndContainer(final String storageSystemName,
            final String containerName, final Pageable pageable) {

        final Map<Long, Cluster> clusterMap = new HashMap<Long, Cluster>();
        this.clusterRepository.findAll().forEach(cluster -> {
            clusterMap.put(cluster.getClusterId(), cluster);
        });

        if (!storageSystemName.equals("All")) {
            final StorageSystem storageSystem = this.storageSystemRepository
                    .findByStorageSystemName(storageSystemName);
            final long storageSystemId = storageSystem.getStorageSystemId();
            if (containerName.equals("All")) {
                return this.pageableSchemaMapRepository.findByStorageSystemId(storageSystemId, pageable);
            }
            else {
                return this.pageableSchemaMapRepository.findByStorageSystemIdAndContainerName(storageSystemId,
                        containerName, pageable);
            }
        }
        else {
            final List<Long> activeStorageSystemIds = this.storageSystemRepository
                    .findByIsActiveYN(ActiveEnumeration.YES.getFlag()).stream()
                    .map(storageSystem -> storageSystem.getStorageSystemId()).collect(Collectors.toList());
            if (containerName.equals("All")) {
                return this.pageableSchemaMapRepository.findByStorageSystemIdIn(activeStorageSystemIds, pageable);
            }
            else {
                return this.pageableSchemaMapRepository.findByContainerNameAndStorageSystemIdIn(activeStorageSystemIds,
                        containerName, pageable);
            }
        }
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED, readOnly = false, rollbackFor = {
            TransactionSystemException.class, DataIntegrityViolationException.class, ValidationError.class,
            IOException.class, InterruptedException.class, ExecutionException.class })
    public ObjectSchemaMap updateObjectSchemaMap(final ObjectSchemaMap schemaMap)
            throws ValidationError, IOException, InterruptedException, ExecutionException {

        final Gson gson = new Gson();
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final String time = sdf.format(timestamp);
        // final String hiveType = "Hive";
        final String defaultUser = "udc_admin";
        final ObjectSchemaMap actualSchemaMap = this.schemaMapUtil
                .validateObjectSchemaMapId(schemaMap.getObjectId());
        if (schemaMap.getObjectSchema() == null) {
            final ValidationError v = new ValidationError();
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("Please Supply ObjectSchema");
            throw v;
        }
        // Get the previous object schema
        final String prev = "{\"value\": " + actualSchemaMap.getObjectSchemaInString()
                + ", \"username\": \""
                + actualSchemaMap.getUpdatedUser()
                + "\"}";
        final String previousSchema = actualSchemaMap.getObjectSchemaInString();
        // get all the Datasets for the given Object
        final List<Dataset> datasetList = this.datasetRepository
                .findByObjectSchemaMapId(actualSchemaMap.getObjectId());

        final boolean datasetExists = datasetList.size() > 0 ? true : false;

        final String previousIsActiveFlag = actualSchemaMap.getIsActiveYN();
        final String isSelfDiscovered = actualSchemaMap.getIsSelfDiscovered() == null
                || actualSchemaMap.getIsSelfDiscovered().equals(ActiveEnumeration.NO.getFlag())
                        ? ActiveEnumeration.NO.getFlag()
                        : ActiveEnumeration.YES.getFlag();

        final String createdUserOnStore = (schemaMap.getCreatedUserOnStore() == null
                || schemaMap.getCreatedUserOnStore().length() == 0)
                && (actualSchemaMap.getCreatedUserOnStore() == null
                        || actualSchemaMap.getCreatedUserOnStore().length() == 0)
                                ? this.unknownString
                                : schemaMap.getCreatedUserOnStore();
        final String createdTimestampOnStore = (schemaMap.getCreatedTimestampOnStore() == null
                || schemaMap.getCreatedTimestampOnStore().length() == 0)
                && (actualSchemaMap.getCreatedTimestampOnStore() == null
                        || actualSchemaMap.getCreatedTimestampOnStore().length() == 0)
                                ? this.unknownString
                                : schemaMap.getCreatedTimestampOnStore();

        final String retrievedQuery = schemaMap.getQuery() == null ? "" : schemaMap.getQuery();
        final String updatedUser = schemaMap.getUpdatedUser() == null
                ? schemaMap.getCreatedUser() == null ? defaultUser : schemaMap.getCreatedUser()
                : schemaMap.getUpdatedUser();

        final ObjectSchemaMap retreivedSchemaMap = new ObjectSchemaMap(actualSchemaMap.getObjectId(),
                schemaMap.getObjectName(), schemaMap.getContainerName(), isSelfDiscovered,
                actualSchemaMap.getIsRegistered(), schemaMap.getStorageSystemId(), ActiveEnumeration.YES.getFlag(),
                createdUserOnStore, createdTimestampOnStore, gson.toJson(schemaMap.getObjectSchema()),
                schemaMap.getObjectSchema(), retrievedQuery, actualSchemaMap.getCreatedUser(),
                actualSchemaMap.getCreatedTimestamp(), updatedUser, time);
        this.schemaMapRepository.save(retreivedSchemaMap);

        // Updated value for the ObjectSchema
        final String curr = "{\"value\": " + retreivedSchemaMap.getObjectSchemaInString()
                + ", \"username\": \""
                + retreivedSchemaMap.getUpdatedUser()
                + "\"}";

        final ObjectMapper mapper = new ObjectMapper();
        final JsonNode old = mapper.readTree(previousSchema);
        final JsonNode newSchema = mapper.readTree(retreivedSchemaMap.getObjectSchemaInString());
        // Update the ObjectSchema for all the datasets related to given object
        for (int i = 0; i < datasetList.size(); i++) {
            if (prev.isEmpty()) {
                final DatasetChangeLog dcl = new DatasetChangeLog(datasetList.get(i).getStorageDataSetId(), "M",
                        TimelineEnumeration.SCHEMA.getFlag(), prev, curr,
                        time);
                this.changeLogRepository.save(dcl);
            }
            if (!old.equals(newSchema)) {
                final DatasetChangeLog dcl = new DatasetChangeLog(datasetList.get(i).getStorageDataSetId(), "M",
                        TimelineEnumeration.SCHEMA.getFlag(), prev, curr,
                        time);
                this.changeLogRepository.save(dcl);
            }
        }
        // update pc_storage_object_attribute_value table
        final List<ObjectAttributeValue> attributes = schemaMap.getObjectAttributes();
        /*
         * if objectAttributes are supplied we would be executing the following else blob
         */

        if (attributes != null && attributes.size() > 0) {
            /*
             * first we will check to see if there are any exisiting object attribute values
             */
            final List<ObjectAttributeValue> retrievedAttributeValues = this.objectAttributeRepository
                    .findByObjectIdAndIsActiveYN(schemaMap.getObjectId(), ActiveEnumeration.YES.getFlag());
            /*
             * if there are no object attribute values for the object, we would blindly insert attribute values
             */
            if (retrievedAttributeValues == null || retrievedAttributeValues.size() == 0) {
                attributes.forEach(attribute -> {
                    final String isCustomized = (attribute.getIsCustomized() == null
                            || attribute.getIsCustomized().equals(ActiveEnumeration.NO.getFlag()))
                                    ? ActiveEnumeration.NO.getFlag()
                                    : attribute.getIsCustomized();
                    final ObjectAttributeValue actualAttribute = new ObjectAttributeValue(
                            attribute.getStorageDsAttributeKeyId(), schemaMap.getObjectId(),
                            attribute.getObjectAttributeValue(), isCustomized, ActiveEnumeration.YES.getFlag(),
                            updatedUser, time, updatedUser, time);
                    this.objectAttributeRepository.save(actualAttribute);

                    if (datasetExists) {
                        this.schemaMapUtil.insertChangeLogForObjectAttribute(null,
                                actualAttribute, datasetList, "C");
                    }

                });

            }

            /*
             * if there are object attributes they we need to update the existing ones
             */
            else {
                attributes.forEach(attr -> {
                    final ObjectAttributeValue retrievedObj = this.objectAttributeRepository
                            .findByStorageDsAttributeKeyIdAndObjectId(attr.getStorageDsAttributeKeyId(),
                                    attr.getObjectId());

                    /*
                     * if an object attribute value exists then we update the record
                     */
                    if (retrievedObj != null) {
                        final String previousObjectAttributeValueName = retrievedObj.getObjectAttributeValue();
                        // if the attribute is not customizable
                        if (retrievedObj.getIsCustomized().equals(ActiveEnumeration.NO.getFlag())
                                || (attr.getIsCustomized() != null
                                        && attr.getIsCustomized().equals(ActiveEnumeration.NO.getFlag()))) {
                            final ObjectAttributeValue currentToBeUpdated = retrievedAttributeValues.stream()
                                    .filter(rAttr -> retrievedObj.getObjectAttributeValueId() == rAttr
                                            .getObjectAttributeValueId())
                                    .collect(Collectors.toList()).get(0);

                            currentToBeUpdated.setUpdatedUser(updatedUser);
                            currentToBeUpdated.setUpdatedTimestamp(time);
                            currentToBeUpdated.setIsCustomized((attr.getIsCustomized() == null
                                    || attr.getIsCustomized().equals(ActiveEnumeration.NO.getFlag()))
                                            ? ActiveEnumeration.NO.getFlag()
                                            : attr.getIsCustomized());
                            currentToBeUpdated.setIsActiveYN(ActiveEnumeration.YES.getFlag());
                            currentToBeUpdated.setObjectAttributeValue(attr.getObjectAttributeValue());
                            this.objectAttributeRepository.save(currentToBeUpdated);

                            if (datasetExists) {
                                final ObjectAttributeValue previousRetrievedObj = new ObjectAttributeValue(
                                        retrievedObj.getObjectAttributeValueId(),
                                        retrievedObj.getStorageDsAttributeKeyId(), retrievedObj.getObjectId(),
                                        previousObjectAttributeValueName,
                                        retrievedObj.getIsCustomized(), retrievedObj.getIsActiveYN(),
                                        retrievedObj.getCreatedUser(), retrievedObj.getCreatedTimestamp(),
                                        updatedUser, retrievedObj.getUpdatedTimestamp());
                                this.schemaMapUtil.insertChangeLogForObjectAttribute(previousRetrievedObj,
                                        currentToBeUpdated, datasetList, "M");
                            }
                        }

                    }
                    /*
                     * else we blindly insert the new property
                     */
                    else {
                        final String isCustomized = attr.getIsCustomized() == null ? ActiveEnumeration.NO.getFlag()
                                : attr.getIsCustomized();
                        final ObjectAttributeValue actualAttribute = new ObjectAttributeValue(
                                attr.getStorageDsAttributeKeyId(), schemaMap.getObjectId(),
                                attr.getObjectAttributeValue(), isCustomized, ActiveEnumeration.YES.getFlag(),
                                updatedUser, time, updatedUser, time);
                        this.objectAttributeRepository.save(actualAttribute);

                        if (datasetExists) {
                            this.schemaMapUtil.insertChangeLogForObjectAttribute(retrievedObj,
                                    actualAttribute, datasetList, "C");
                        }
                    }
                });
            }
        }

        final List<Dataset> datasets = this.datasetRepository
                .findByObjectSchemaMapId(retreivedSchemaMap.getObjectId());
        if (previousIsActiveFlag.equals(ActiveEnumeration.NO.getFlag())) {
            if (datasets != null && datasets.size() > 0) {
                datasets.forEach(dataset -> {
                    dataset.setIsActiveYN(ActiveEnumeration.YES.getFlag());
                    dataset.setUpdatedTimestamp(time);
                });
                this.datasetRepository.saveAll(datasets);

                datasets.forEach(dataset -> {
                    final DatasetStorageSystem datasetSystem = this.datasetSystemRepository
                            .findByStorageDataSetId(dataset.getStorageDataSetId());
                    datasetSystem.setIsActiveYN(ActiveEnumeration.YES.getFlag());
                    datasetSystem.setUpdatedTimestamp(time);
                    this.datasetSystemRepository.save(datasetSystem);
                });

            }
        }

        if (this.isEsWriteEnabled.equals("true")) {
            for (final Dataset dataset : datasets) {
                this.datasetUtil.upsertDataset(this.esDatasetIndex, this.esType, dataset, this.esTemplate);
            }
        }

        return retreivedSchemaMap;
    }

    @Override
    public List<CollectiveObjectSchemaMap> getSchemaBySystemContainerAndObject(final long systemId,
            final String containerName,
            final String objectName) {

        final Gson gson = new Gson();
        final List<CollectiveObjectSchemaMap> objects = new ArrayList<CollectiveObjectSchemaMap>();
        final ObjectSchemaMap objectSchemaMap = this.schemaMapRepository
                .findByStorageSystemIdAndContainerNameAndObjectName(systemId, containerName, objectName);
        if (objectSchemaMap != null) {
            final List<Long> clusterIds = this.clusterUtil.getAllClusters().stream()
                    .map(cluster -> cluster.getClusterId())
                    .collect(Collectors.toList());
            final List<ObjectAttributeValue> objectAttributeValues = this.objectAttributeRepository
                    .findByObjectIdAndIsActiveYN(objectSchemaMap.getObjectId(), ActiveEnumeration.YES.getFlag());
            final List<CollectiveObjectAttributeValue> attributeValues = new ArrayList<CollectiveObjectAttributeValue>();
            objectAttributeValues.forEach(attr -> {
                final CollectiveObjectAttributeValue tempAttr = new CollectiveObjectAttributeValue();
                tempAttr.setStorageDsAttributeKeyId(attr.getStorageDsAttributeKeyId());
                tempAttr.setObjectAttributeValue(attr.getObjectAttributeValue());
                tempAttr.setObjectId(objectSchemaMap.getObjectId());
                tempAttr.setIsCustomized(attr.getIsCustomized());
                attributeValues.add(tempAttr);
            });

            final String schema = objectSchemaMap.getObjectSchemaInString();
            List<Schema> modifiedSchema;
            if (schema == null || schema.length() == 0) {
                modifiedSchema = new ArrayList<Schema>();
            }
            else {
                modifiedSchema = gson.fromJson(schema, new TypeToken<List<Schema>>() {
                }.getType());
            }
            final CollectiveObjectSchemaMap object = new CollectiveObjectSchemaMap(objectSchemaMap.getObjectId(),
                    objectSchemaMap.getObjectName(), objectSchemaMap.getContainerName(),
                    objectSchemaMap.getStorageSystemId(), clusterIds,
                    objectSchemaMap.getQuery(), modifiedSchema, attributeValues,
                    objectSchemaMap.getIsActiveYN(), objectSchemaMap.getCreatedUserOnStore(),
                    objectSchemaMap.getCreatedTimestampOnStore(), objectSchemaMap.getIsSelfDiscovered());
            objects.add(object);
        }
        return objects;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED, readOnly = false, rollbackFor = {
            TransactionSystemException.class, DataIntegrityViolationException.class, ValidationError.class })
    public void deActivateObjectAndDataset(final long objectId) throws ValidationError {

        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final String time = sdf.format(timestamp);
        final String hiveType = "Hive";
        // deactivate object in pc_object_schema_map table
        final ObjectSchemaMap objectSchema = this.schemaMapUtil.validateObjectSchemaMapId(objectId);
        objectSchema.setIsActiveYN(ActiveEnumeration.NO.getFlag());
        objectSchema.setUpdatedTimestamp(time);
        this.schemaMapRepository.save(objectSchema);

        // deactivate in pc_storage_dataset table
        final List<Dataset> datasets = this.datasetRepository.findByObjectSchemaMapId(objectId);
        datasets.forEach(dataset -> {
            dataset.setIsActiveYN(ActiveEnumeration.NO.getFlag());
            dataset.setUpdatedTimestamp(time);
        });
        this.datasetRepository.saveAll(datasets);

        // deactivate in pc_storage_dataset_system table
        datasets.forEach(dataset -> {
            final DatasetStorageSystem datasetSystem = this.datasetSystemRepository
                    .findByStorageDataSetId(dataset.getStorageDataSetId());
            datasetSystem.setIsActiveYN(ActiveEnumeration.NO.getFlag());
            datasetSystem.setUpdatedTimestamp(time);
            this.datasetSystemRepository.save(datasetSystem);
        });

        // update pc_storage_dataset_change_log table
        datasets.forEach(dataset -> {
            final List<DatasetChangeLog> changeLogList = this.changeLogRepository
                    .findByStorageDataSetIdAndChangeColumnType(dataset.getStorageDataSetId(), "DATASET");
            final DatasetChangeLog tempChangeLogDataset = changeLogList.get(changeLogList.size() - 1);

            final DatasetChangeLog dcl = new DatasetChangeLog(dataset.getStorageDataSetId(), "D",
                    "DATASET", tempChangeLogDataset.getColumnCurrValInString(), "{}",
                    sdf.format(timestamp));
            this.changeLogRepository.save(dcl);
        });
    }

    @Override
    public Page<ObjectSchemaMap> getObjectsByStorageSystem(final String objectStr, final String storageSystemName,
            final Pageable pageable) {

        if (!storageSystemName.equals("All")) {
            final StorageSystem storageSystem = this.storageSystemRepository
                    .findByStorageSystemName(storageSystemName);
            final long storageSystemId = storageSystem.getStorageSystemId();
            if (objectStr.equals("All")) {
                return this.pageableSchemaMapRepository.findByStorageSystemId(storageSystemId,
                        pageable);
            }
            else {
                return this.pageableSchemaMapRepository.findByStorageSystemIdAndObjectNameContaining(storageSystemId,
                        objectStr, pageable);
            }

        }
        else {
            final List<Long> activeStorageSystemIds = this.storageSystemRepository
                    .findByIsActiveYN(ActiveEnumeration.YES.getFlag()).stream()
                    .map(storageSystem -> storageSystem.getStorageSystemId()).collect(Collectors.toList());
            if (objectStr.equals("All")) {
                return this.pageableSchemaMapRepository.findByStorageSystemIdIn(activeStorageSystemIds,
                        pageable);
            }
            else {
                return this.pageableSchemaMapRepository.findByStorageSystemIdInAndObjectNameContaining(
                        activeStorageSystemIds, objectStr, pageable);
            }
        }
    }

    @Override
    public List<ObjectAttributeKeyValue> getCustomAttributesByObject(final long objectId) {
        return this.sackvr.findByObjectId(objectId);
    }

    @Override
    public List<Dataset> getDatasetByObject(final String objectName) {
        final List<Dataset> datasets = this.schemaMapUtil.getDatasetByObject(objectName);
        return datasets;
    }

}
