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

package com.paypal.udc.util;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import com.google.gson.Gson;
import com.paypal.udc.dao.objectschema.ObjectSchemaAttributeValueRepository;
import com.paypal.udc.dao.objectschema.ObjectSchemaMapRepository;
import com.paypal.udc.dao.storagecategory.StorageRepository;
import com.paypal.udc.dao.storagesystem.StorageSystemAttributeValueRepository;
import com.paypal.udc.dao.storagesystem.StorageSystemRepository;
import com.paypal.udc.dao.storagetype.ElasticSearchStorageTypeRepository;
import com.paypal.udc.dao.storagetype.StorageTypeAttributeKeyRepository;
import com.paypal.udc.dao.storagetype.StorageTypeRepository;
import com.paypal.udc.entity.objectschema.ObjectAttributeValue;
import com.paypal.udc.entity.objectschema.ObjectSchemaMap;
import com.paypal.udc.entity.storagecategory.Storage;
import com.paypal.udc.entity.storagesystem.StorageSystem;
import com.paypal.udc.entity.storagesystem.StorageSystemAttributeValue;
import com.paypal.udc.entity.storagetype.CollectiveStorageTypeAttributeKey;
import com.paypal.udc.entity.storagetype.ElasticSearchStorageType;
import com.paypal.udc.entity.storagetype.StorageType;
import com.paypal.udc.entity.storagetype.StorageTypeAttributeKey;
import com.paypal.udc.exception.ValidationError;
import com.paypal.udc.util.enumeration.ActiveEnumeration;


@Component
public class StorageTypeUtil {

    final static Logger logger = LoggerFactory.getLogger(StorageTypeUtil.class);
    @Autowired
    private StorageRepository storageRepository;

    @Autowired
    private StorageTypeRepository storageTypeRepository;

    @Autowired
    private ElasticSearchStorageTypeRepository esStorageTypeRepository;
    @Autowired
    private StorageTypeAttributeKeyRepository storageTypeAttributeRepository;

    @Autowired
    private ObjectSchemaMapRepository objectSchemaMapRepository;

    @Autowired
    private StorageSystemRepository storageSystemRepository;

    @Autowired
    private ObjectSchemaAttributeValueRepository osavr;

    @Autowired
    private StorageSystemAttributeValueRepository ssavr;

    @Autowired
    private TransportClient client;

    @Autowired
    private StorageTypeAttributeKeyRepository storageAttributeRepository;

    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss");

    @Autowired
    private StorageSystemUtil storageSystemUtil;
    @Value("${elasticsearch.type.name}")
    private String esType;

    @Value("${udc.es.write.enabled}")
    private String isEsWriteEnabled;

    @Value("${elasticsearch.system.index.name}")
    private String esSystemIndex;

    @Autowired
    private ElasticsearchTemplate esTemplate;

    public StorageType getStorageType(final long storageTypeId) throws ValidationError {
        final StorageType storageType = this.storageTypeRepository.findById(storageTypeId).orElse(null);
        if (storageType == null) {
            final ValidationError verror = new ValidationError();
            verror.setErrorCode(HttpStatus.BAD_REQUEST);
            verror.setErrorDescription("Invalid Storage Type");
            throw verror;
        }
        final Storage storage = this.storageRepository.findById(storageType.getStorageId()).orElse(null);
        storageType.setStorage(storage);
        return storageType;
    }

    public Map<Long, Storage> getStorages() {
        final Map<Long, Storage> storages = new HashMap<Long, Storage>();
        this.storageRepository.findAll().forEach(
                storage -> storages.put(storage.getStorageId(), storage));
        return storages;
    }

    public StorageType validateStorageTypeId(final long id) throws ValidationError {
        final StorageType storageType = this.storageTypeRepository.findById(id).orElse(null);
        if (storageType == null) {
            final ValidationError verror = new ValidationError();
            verror.setErrorCode(HttpStatus.BAD_REQUEST);
            verror.setErrorDescription("Invalid Storage Type");
            throw verror;
        }
        return storageType;
    }

    public Map<Long, StorageType> getStorageTypes() {
        final Map<Long, StorageType> storageTypeMap = new HashMap<Long, StorageType>();
        final Map<Long, Storage> storageMap = this.getStorages();
        this.storageTypeRepository.findAll()
                .forEach(storageType -> {
                    final Storage storage = storageMap.get(storageType.getStorageId());
                    storageType.setStorage(storage);
                    storageTypeMap.put(storageType.getStorageTypeId(), storageType);
                });
        return storageTypeMap;
    }

    public Map<Long, StorageTypeAttributeKey> getStorageTypeAttributes() {

        final Map<Long, StorageTypeAttributeKey> storageTypeAttributeMap = new HashMap<Long, StorageTypeAttributeKey>();
        this.storageTypeAttributeRepository.findAll().forEach(storageAttribute -> storageTypeAttributeMap
                .put(storageAttribute.getStorageDsAttributeKeyId(), storageAttribute));

        return storageTypeAttributeMap;
    }

    public void validateAttributes(final CollectiveStorageTypeAttributeKey attributeKey) throws ValidationError {
        final String storageDsAttributeName = attributeKey.getStorageDsAttributeKeyName();
        if (storageDsAttributeName == null || storageDsAttributeName.length() == 0) {
            final ValidationError verror = new ValidationError();
            verror.setErrorCode(HttpStatus.BAD_REQUEST);
            verror.setErrorDescription("Invalid Storage Type Attribute Name");
            throw verror;
        }
        final String storageDsAttributeDescription = attributeKey.getStorageDsAttributeKeyDesc();
        if (storageDsAttributeDescription == null || storageDsAttributeDescription.length() == 0) {
            final ValidationError verror = new ValidationError();
            verror.setErrorCode(HttpStatus.BAD_REQUEST);
            verror.setErrorDescription("Invalid Storage Type Attribute Description");
            throw verror;
        }
    }

    public void insertAttributesForExistingObjects(final StorageTypeAttributeKey stak) {

        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final String time = sdf.format(timestamp);

        final List<Long> storageSystemIds = this.storageSystemRepository
                .findByStorageTypeId(stak.getStorageTypeId())
                .stream().map(stakr -> stakr.getStorageSystemId()).collect(Collectors.toList());
        if (stak.getIsStorageSystemLevel().equals(ActiveEnumeration.YES.getFlag())) {
            if (storageSystemIds != null && storageSystemIds.size() > 0) {
                final List<StorageSystemAttributeValue> ssavs = new ArrayList<StorageSystemAttributeValue>();
                storageSystemIds.forEach(storageSystemId -> {
                    final StorageSystemAttributeValue ssav = new StorageSystemAttributeValue();
                    ssav.setCreatedTimestamp(time);
                    ssav.setCreatedUser(stak.getCreatedUser());
                    ssav.setUpdatedUser(stak.getUpdatedUser());
                    ssav.setUpdatedTimestamp(time);
                    ssav.setIsActiveYN(ActiveEnumeration.YES.getFlag());
                    ssav.setStorageSystemID(storageSystemId);
                    ssav.setStorageDataSetAttributeKeyId(stak.getStorageDsAttributeKeyId());
                    ssav.setStorageSystemAttributeValue("Defaults");
                    ssavs.add(ssav);
                });
                this.ssavr.saveAll(ssavs);
            }
        }
        if (stak.getIsStorageSystemLevel().equals(ActiveEnumeration.NO.getFlag())) {
            final List<ObjectSchemaMap> objects = this.objectSchemaMapRepository
                    .findByStorageSystemIdIn(storageSystemIds);
            if (objects != null && objects.size() > 0) {
                final List<Long> objectIds = objects.stream().map(object -> object.getObjectId())
                        .collect(Collectors.toList());
                final List<ObjectAttributeValue> oavs = new ArrayList<ObjectAttributeValue>();
                objectIds.forEach(objectId -> {
                    final ObjectAttributeValue oav = new ObjectAttributeValue();
                    oav.setObjectId(objectId);
                    oav.setObjectAttributeValue("Defaults");
                    oav.setStorageDsAttributeKeyId(stak.getStorageDsAttributeKeyId());
                    oav.setIsCustomized(ActiveEnumeration.NO.getFlag());
                    oav.setIsActiveYN(ActiveEnumeration.YES.getFlag());
                    oav.setCreatedTimestamp(time);
                    oav.setUpdatedTimestamp(time);
                    oav.setCreatedUser(stak.getCreatedUser());
                    oav.setUpdatedUser(stak.getUpdatedUser());
                    oavs.add(oav);
                });
                this.osavr.saveAll(oavs);
            }
        }

    }

    public void updateAttributeKeys(final List<StorageTypeAttributeKey> attributeKeys)
            throws ValidationError, IOException, InterruptedException, ExecutionException {
        final List<Long> storageSystemIds = this.storageSystemRepository
                .findByStorageTypeId(attributeKeys.get(0).getStorageTypeId())
                .stream().map(stakr -> stakr.getStorageSystemId()).collect(Collectors.toList());
        // final List<ObjectSchemaMap> objects = this.objectSchemaMapRepository
        // .findByStorageSystemIdIn(storageSystemIds);

        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final String time = sdf.format(timestamp);

        final List<StorageTypeAttributeKey> systemAttributeKeys = attributeKeys.stream()
                .filter(key -> key.getIsStorageSystemLevel().equals(ActiveEnumeration.YES.getFlag()))
                .collect(Collectors.toList());
        if (systemAttributeKeys != null && systemAttributeKeys.size() > 0) {
            int i, j = 0;
            for (i = 0; i < systemAttributeKeys.size(); i++) {
                final List<StorageSystemAttributeValue> ssavs = new ArrayList<StorageSystemAttributeValue>();
                for (j = 0; j < storageSystemIds.size(); j++) {
                    StorageSystem storageSystem = new StorageSystem();
                    storageSystem = this.storageSystemUtil.validateStorageSystem(storageSystemIds.get(j));
                    final StorageSystemAttributeValue ssav = this.ssavr
                            .findByStorageSystemIdAndStorageDataSetAttributeKeyId(storageSystemIds.get(j),
                                    systemAttributeKeys.get(i).getStorageDsAttributeKeyId());
                    ssav.setIsActiveYN(systemAttributeKeys.get(i).getIsActiveYN());
                    ssav.setUpdatedUser(systemAttributeKeys.get(i).getUpdatedUser());
                    ssav.setUpdatedTimestamp(time);
                    ssavs.add(ssav);
                    if (this.isEsWriteEnabled.equals("true")) {
                        this.storageSystemUtil.upsertStorageSystem(this.esSystemIndex, this.esType,
                                storageSystem,
                                this.esTemplate);
                    }
                }
                this.ssavr.saveAll(ssavs);
            }
        }
        // final List<StorageTypeAttributeKey> objectAttributeKeys = attributeKeys.stream()
        // .filter(key -> key.getIsStorageSystemLevel().equals(ActiveEnumeration.NO.getFlag()))
        // .collect(Collectors.toList());
        // final List<Long> objectIds = objects.stream().map(object -> object.getObjectId())
        // .collect(Collectors.toList());
        // if (objectAttributeKeys != null && objectAttributeKeys.size() > 0) {
        // objectAttributeKeys.forEach(objectAttributeKey -> {
        // final List<ObjectAttributeValue> oavs = new ArrayList<ObjectAttributeValue>();
        // objectIds.forEach(objectId -> {
        // final ObjectAttributeValue oav = this.osavr.findByObjectIdAndStorageDsAttributeKeyId(
        // objectId, objectAttributeKey.getStorageDsAttributeKeyId());
        // oav.setIsActiveYN(objectAttributeKey.getIsActiveYN());
        // oav.setUpdatedUser(objectAttributeKey.getUpdatedUser());
        // oav.setUpdatedTimestamp(time);
        // oavs.add(oav);
        // });
        // this.osavr.save(oavs);
        // });
        // }
    }

    public void save(final String indexName, final String typeName, final StorageType storageType) {

        final Gson gson = new Gson();
        final List<StorageTypeAttributeKey> attributeKeys = this.storageAttributeRepository
                .findByStorageTypeId(storageType.getStorageTypeId());

        final ElasticSearchStorageType esStorageType = new ElasticSearchStorageType(storageType.getStorageTypeId(),
                storageType.getStorageTypeName(), storageType.getStorageTypeDescription(), storageType.getCreatedUser(),
                storageType.getCreatedTimestamp(), storageType.getUpdatedUser(), storageType.getUpdatedTimestamp(),
                storageType.getStorageId(), storageType.getIsActiveYN(), attributeKeys);

        final IndexResponse response = this.client.prepareIndex(indexName, typeName, esStorageType.get_id())
                .setSource(gson.toJson(esStorageType), XContentType.JSON)
                .get();
    }

    public void update(final String indexName, final String typeName,
            final ElasticSearchStorageType esStorageType,
            final StorageType storageType) throws IOException, InterruptedException, ExecutionException {

        final Gson gson = new Gson();
        final List<StorageTypeAttributeKey> attributeKeys = this.storageAttributeRepository
                .findByStorageTypeId(storageType.getStorageTypeId());

        storageType.setAttributeKeys(attributeKeys);
        final UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index(indexName).type(typeName)
                .id(esStorageType.get_id())
                .doc(gson.toJson(storageType), XContentType.JSON);
        final UpdateResponse updateResponse = this.client.update(updateRequest).get();
    }

    public void upsertStorageType(final String esTypeIndex, final String esType, final StorageType storageType,
            final ElasticsearchTemplate esTemplate) throws IOException, InterruptedException, ExecutionException {
        final List<ElasticSearchStorageType> esStorageType = this.esStorageTypeRepository
                .findByStorageTypeId((storageType.getStorageTypeId()));

        if (esStorageType == null || esStorageType.isEmpty()) {
            this.save(esTypeIndex, esType, storageType);
            esTemplate.refresh(esTypeIndex);
        }
        else {
            this.update(esTypeIndex, esType, esStorageType.get(0), storageType);
            esTemplate.refresh(esTypeIndex);
        }
    }
}
