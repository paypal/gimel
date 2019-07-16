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

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.paypal.udc.dao.dataset.DatasetChangeLogRepository;
import com.paypal.udc.dao.dataset.DatasetRepository;
import com.paypal.udc.dao.objectschema.ObjectSchemaAttributeValueRepository;
import com.paypal.udc.dao.objectschema.ObjectSchemaMapRepository;
import com.paypal.udc.dao.storagetype.StorageTypeAttributeKeyRepository;
import com.paypal.udc.entity.dataset.Dataset;
import com.paypal.udc.entity.dataset.DatasetChangeLog;
import com.paypal.udc.entity.objectschema.CollectiveObjectAttributeValue;
import com.paypal.udc.entity.objectschema.CollectiveObjectSchemaMap;
import com.paypal.udc.entity.objectschema.ObjectAttributeValue;
import com.paypal.udc.entity.objectschema.ObjectSchemaMap;
import com.paypal.udc.entity.objectschema.Schema;
import com.paypal.udc.entity.storagetype.StorageType;
import com.paypal.udc.entity.storagetype.StorageTypeAttributeKey;
import com.paypal.udc.exception.ValidationError;
import com.paypal.udc.service.impl.ObjectSchemaMapService;
import com.paypal.udc.util.enumeration.ActiveEnumeration;
import com.paypal.udc.util.enumeration.TimelineEnumeration;


@Component
public class ObjectSchemaMapUtil {

    @Autowired
    private StorageTypeAttributeKeyRepository stakr;

    @Autowired
    private ObjectSchemaMapRepository schemaMapRepository;

    @Autowired
    private StorageSystemUtil systemUtil;

    @Autowired
    private DatasetChangeLogRepository changeLogRepository;

    @Autowired
    private DatasetRepository datasetRepository;

    final static Logger logger = LoggerFactory.getLogger(ObjectSchemaMapService.class);

    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss");

    public StorageTypeAttributeKey getStorageTypeAttributeValue(final String attributeName) {
        final StorageTypeAttributeKey attributeKey = this.stakr.findByStorageDsAttributeKeyName(attributeName);
        return attributeKey;
    }

    public void updateObject(final long objectSchemaMapId) throws ValidationError {
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final String time = sdf.format(timestamp);
        final ObjectSchemaMap schemaMap = this.schemaMapRepository.findById(objectSchemaMapId)
                .orElse(null);
        if (schemaMap == null) {
            final ValidationError verror = new ValidationError();
            verror.setErrorCode(HttpStatus.CONFLICT);
            verror.setErrorDescription("Invalid Schema Map Id");
            throw verror;
        }

        // schemaMap.setIsRegistered(ActiveEnumeration.YES.getFlag());
        schemaMap.setUpdatedTimestamp(time);
        this.schemaMapRepository.save(schemaMap);
    }

    public ObjectSchemaMap validateObjectSchemaMapId(final long objectSchemaMapId) throws ValidationError {
        final ObjectSchemaMap osm = this.schemaMapRepository.findById(objectSchemaMapId).orElse(null);
        if (osm == null) {
            final ValidationError verror = new ValidationError();
            verror.setErrorCode(HttpStatus.CONFLICT);
            verror.setErrorDescription("Invalid Schema Map Id");
            throw verror;
        }
        return osm;
    }

    public ObjectSchemaMap validateSystemContainerObjectMapping(final long storageSystemId,
            final String storageContainerName, final String objectName) throws ValidationError {
        final ObjectSchemaMap objectSchemaMap = this.schemaMapRepository
                .findByStorageSystemIdAndContainerNameAndObjectName(storageSystemId, storageContainerName, objectName);
        if (objectSchemaMap == null) {
            final ValidationError verror = new ValidationError();
            verror.setErrorCode(HttpStatus.BAD_REQUEST);
            verror.setErrorDescription("Invalid Mapping between System, Container and Object");
            throw verror;
        }
        return objectSchemaMap;
    }

    public void updateObjectAutoRegistrationStatus(final long objectSchemaMapId,
            final boolean allAttrPresent, final String storageTypeName) throws ValidationError {
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final String time = sdf.format(timestamp);
        final ObjectSchemaMap schemaMap = this.schemaMapRepository.findById(objectSchemaMapId)
                .orElse(null);
        if (schemaMap == null) {
            final ValidationError verror = new ValidationError();
            verror.setErrorCode(HttpStatus.CONFLICT);
            verror.setErrorDescription("Invalid Schema Map Id");
            throw verror;
        }
        schemaMap.setIsRegistered(
                allAttrPresent ? ActiveEnumeration.YES.getFlag() : ActiveEnumeration.PENDING.getFlag());
        schemaMap.setUpdatedTimestamp(time);
        this.schemaMapRepository.save(schemaMap);
    }

    public StorageType getTypeFromObject(final ObjectSchemaMap object) throws ValidationError {
        final long storageSystemId = object.getStorageSystemId();
        final StorageType type = this.systemUtil.getStorageType(storageSystemId);
        return type;
    }

    public List<CollectiveObjectSchemaMap> getPagedObjectSchemaMaps(final List<ObjectSchemaMap> objects,
            final Gson gson, final ObjectSchemaMapRepository schemaMapRepository, final Pageable pageable,
            final ObjectSchemaAttributeValueRepository objectAttributeRepository, final long systemId,
            final List<Long> clusters) {
        final List<CollectiveObjectSchemaMap> modifiedObjects = new ArrayList<CollectiveObjectSchemaMap>();
        objects.stream().forEach(object -> {
            final List<ObjectAttributeValue> attributeValues = objectAttributeRepository
                    .findByObjectIdAndIsActiveYN(object.getObjectId(),
                            ActiveEnumeration.YES.getFlag());
            final String objectSchema = object.getObjectSchemaInString();
            List<Schema> modifiedSchema = new ArrayList<Schema>();
            if (objectSchema != null && objectSchema.length() > 0) {
                modifiedSchema = gson.fromJson(objectSchema, new TypeToken<List<Schema>>() {
                }.getType());
            }
            final List<CollectiveObjectAttributeValue> collectiveObjectAttributeValues = new ArrayList<CollectiveObjectAttributeValue>();
            attributeValues.forEach(attr -> {
                final CollectiveObjectAttributeValue tempAttr = new CollectiveObjectAttributeValue();
                tempAttr.setStorageDsAttributeKeyId(attr.getStorageDsAttributeKeyId());
                tempAttr.setObjectAttributeValue(attr.getObjectAttributeValue());
                tempAttr.setObjectId(object.getObjectId());
                tempAttr.setIsCustomized(attr.getIsCustomized());
                collectiveObjectAttributeValues.add(tempAttr);
            });
            final CollectiveObjectSchemaMap collectiveObjectSchemaMap = new CollectiveObjectSchemaMap(
                    object.getObjectId(), object.getObjectName(), object.getContainerName(),
                    systemId, clusters, object.getQuery(), modifiedSchema, collectiveObjectAttributeValues,
                    object.getIsActiveYN(), object.getCreatedUserOnStore(), object.getCreatedTimestampOnStore(),
                    object.getIsSelfDiscovered());
            modifiedObjects.add(collectiveObjectSchemaMap);
        });

        return modifiedObjects;

    }

    public void validateObjectAttributeKey(final String objectAttributeKey, final ObjectSchemaMap objectSchemaMap)
            throws ValidationError {
        final StorageType storageType = this.getTypeFromObject(objectSchemaMap);
        final List<StorageTypeAttributeKey> typeAttributeKeys = this.stakr
                .findByStorageTypeIdAndIsActiveYN(storageType.getStorageTypeId(), ActiveEnumeration.YES.getFlag());

        final List<StorageTypeAttributeKey> filteredTypeAttributeKeys = typeAttributeKeys.stream()
                .filter(attributeKey -> attributeKey.getStorageDsAttributeKeyName().equals(objectAttributeKey))
                .collect(Collectors.toList());

        if (filteredTypeAttributeKeys.size() > 0) {
            final ValidationError verror = new ValidationError();
            verror.setErrorCode(HttpStatus.CONFLICT);
            verror.setErrorDescription("Object Attribute at type level already exists. Please try to reuse");
            throw verror;
        }
    }

    public void insertChangeLogForObjectAttribute(final ObjectAttributeValue retrievedObj,
            final ObjectAttributeValue currentToBeUpdated,
            final List<Dataset> datasetList, final String changeType) {
        final StorageTypeAttributeKey storageAttribute = this.stakr
                .findById(retrievedObj.getStorageDsAttributeKeyId())
                .orElse(null);
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final String time = sdf.format(timestamp);
        final Gson gson = new Gson();

        final Map<String, String> previousKeyValue = new HashMap<String, String>();
        previousKeyValue.put("keyName", storageAttribute.getStorageDsAttributeKeyName());
        previousKeyValue.put("keyValue", StringEscapeUtils
                .escapeJava(retrievedObj.getObjectAttributeValue().replaceAll("[\\n|\\t|\\s|\\r]", "")));
        previousKeyValue.put("username", retrievedObj.getUpdatedUser());
        final String prev = gson.toJson(previousKeyValue);
        final Map<String, String> currentKeyValue = new HashMap<String, String>();
        currentKeyValue.put("keyName", storageAttribute.getStorageDsAttributeKeyName());
        currentKeyValue.put("keyValue", StringEscapeUtils
                .escapeJava(currentToBeUpdated.getObjectAttributeValue().replaceAll("[\\n|\\t|\\s|\\r]", "")));
        currentKeyValue.put("username", currentToBeUpdated.getUpdatedUser());

        final String curr = gson.toJson(currentKeyValue);
        if (!(previousKeyValue.get("keyName").equals(currentKeyValue.get("keyName"))
                && previousKeyValue.get("keyValue").equals(currentKeyValue.get("keyValue")))) {
            for (int i = 0; i < datasetList.size(); i++) {
                final DatasetChangeLog dcl = new DatasetChangeLog(
                        datasetList.get(i).getStorageDataSetId(), changeType,
                        TimelineEnumeration.OBJECT_ATTRIBUTES.getFlag(), prev,
                        curr, time);
                this.changeLogRepository.save(dcl);
            }
        }
    }

    public List<Dataset> getDatasetByObject(final String objectName) {
        final List<ObjectSchemaMap> schemas = this.schemaMapRepository.findByObjectNameContaining(objectName);
        if (schemas == null || schemas.size() == 0) {
            return new ArrayList<Dataset>();
        }
        final List<Long> schemaMapIds = schemas.stream().map(schema -> schema.getObjectId())
                .collect(Collectors.toList());
        final List<Dataset> datasets = this.datasetRepository.findByObjectSchemaMapIdIn(schemaMapIds);
        return datasets;
    }
}
