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

package com.paypal.udc.entity.dataset;

import java.util.List;
import javax.persistence.Transient;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import com.paypal.udc.entity.integration.common.Tag;
import com.paypal.udc.entity.integration.description.SourceProviderDatasetMap;
import com.paypal.udc.entity.objectschema.ObjectAttributeKeyValue;
import com.paypal.udc.entity.objectschema.ObjectAttributeValue;
import com.paypal.udc.entity.objectschema.Schema;
import com.paypal.udc.entity.ownership.DatasetOwnershipMap;


@Document(indexName = "#{@datasetIndexName}", type = "udc")
public class ElasticSearchDataset {

    @Id
    private String _id;
    private long storageDataSetId;
    private String storageDataSetName;
    private String storageDataSetAliasName;
    private String storageDatabaseName;
    private String storageDataSetDescription;
    private String isActiveYN;
    private long userId;
    private String createdUser;
    private String createdTimestamp;
    private String updatedUser;
    private String updatedTimestamp;
    private String isAutoRegistered;
    private long objectSchemaMapId;
    private long storageSystemId;
    private long zoneId;
    private String isReadCompatible;
    private String containerName;
    private String objectName;
    private String isSelfDiscovered;
    private String isRegistered;
    private String createdUserOnStore;
    private String createdTimestampOnStore;
    private List<Schema> objectSchema;
    private List<ObjectAttributeValue> objectAttributeValues;
    private List<Tag> tags;
    private List<DatasetOwnershipMap> ownerships;
    private List<SourceProviderDatasetMap> objectDescriptions;
    private List<ObjectAttributeKeyValue> customAttributeValues;
    @Transient
    private String storageSystemName;
    @Transient
    private String isAccessControlled;
    @Transient
    private String zoneName;
    @Transient
    private String entityName;

    public List<DatasetOwnershipMap> getOwnerships() {
        return this.ownerships;
    }

    public void setOwnerships(final List<DatasetOwnershipMap> ownerships) {
        this.ownerships = ownerships;
    }

    public String getEntityName() {
        return this.entityName;
    }

    public void setEntityName(final String entityName) {
        this.entityName = entityName;
    }

    public String getZoneName() {
        return this.zoneName;
    }

    public void setZoneName(final String zoneName) {
        this.zoneName = zoneName;
    }

    public String getIsAccessControlled() {
        return this.isAccessControlled;
    }

    public void setIsAccessControlled(final String isAccessControlled) {
        this.isAccessControlled = isAccessControlled;
    }

    public String getStorageSystemName() {
        return this.storageSystemName;
    }

    public void setStorageSystemName(final String storageSystemName) {
        this.storageSystemName = storageSystemName;
    }

    public List<ObjectAttributeKeyValue> getCustomAttributeValues() {
        return this.customAttributeValues;
    }

    public void setCustomAttributeValues(final List<ObjectAttributeKeyValue> customAttributeValues) {
        this.customAttributeValues = customAttributeValues;
    }

    public List<Tag> getTags() {
        return this.tags;
    }

    public void setTags(final List<Tag> tags) {
        this.tags = tags;
    }

    public List<SourceProviderDatasetMap> getObjectDescriptions() {
        return this.objectDescriptions;
    }

    public void setObjectDescriptions(final List<SourceProviderDatasetMap> objectDescriptions) {
        this.objectDescriptions = objectDescriptions;
    }

    public long getZoneId() {
        return this.zoneId;
    }

    public void setZoneId(final long zoneId) {
        this.zoneId = zoneId;
    }

    public List<ObjectAttributeValue> getObjectAttributeValues() {
        return this.objectAttributeValues;
    }

    public void setObjectAttributeValues(final List<ObjectAttributeValue> objectAttributeValues) {
        this.objectAttributeValues = objectAttributeValues;
    }

    public String getIsSelfDiscovered() {
        return this.isSelfDiscovered;
    }

    public void setIsSelfDiscovered(final String isSelfDiscovered) {
        this.isSelfDiscovered = isSelfDiscovered;
    }

    public String getIsRegistered() {
        return this.isRegistered;
    }

    public void setIsRegistered(final String isRegistered) {
        this.isRegistered = isRegistered;
    }

    public String getCreatedUserOnStore() {
        return this.createdUserOnStore;
    }

    public void setCreatedUserOnStore(final String createdUserOnStore) {
        this.createdUserOnStore = createdUserOnStore;
    }

    public String getCreatedTimestampOnStore() {
        return this.createdTimestampOnStore;
    }

    public void setCreatedTimestampOnStore(final String createdTimestampOnStore) {
        this.createdTimestampOnStore = createdTimestampOnStore;
    }

    public List<Schema> getObjectSchema() {
        return this.objectSchema;
    }

    public void setObjectSchema(final List<Schema> objectSchema) {
        this.objectSchema = objectSchema;
    }

    public String getIsReadCompatible() {
        return this.isReadCompatible;
    }

    public void setIsReadCompatible(final String isReadCompatible) {
        this.isReadCompatible = isReadCompatible;
    }

    public String getObjectName() {
        return this.objectName;
    }

    public void setObjectName(final String objectName) {
        this.objectName = objectName;
    }

    public long getObjectSchemaMapId() {
        return this.objectSchemaMapId;
    }

    public void setObjectSchemaMapId(final long objectSchemaMapId) {
        this.objectSchemaMapId = objectSchemaMapId;
    }

    public String getStorageDataSetAliasName() {
        return this.storageDataSetAliasName;
    }

    public void setStorageDataSetAliasName(final String storageDataSetAliasName) {
        this.storageDataSetAliasName = storageDataSetAliasName;
    }

    public String getIsActiveYN() {
        return this.isActiveYN;
    }

    public void setIsActiveYN(final String isActiveYN) {
        this.isActiveYN = isActiveYN;
    }

    public long getUserId() {
        return this.userId;
    }

    public void setUserId(final long userId) {
        this.userId = userId;
    }

    public long getStorageSystemId() {
        return this.storageSystemId;
    }

    public void setStorageSystemId(final long storageSystemId) {
        this.storageSystemId = storageSystemId;
    }

    public long getStorageDataSetId() {
        return this.storageDataSetId;
    }

    public void setStorageDataSetId(final long storageDataSetId) {
        this.storageDataSetId = storageDataSetId;
    }

    public String getStorageDataSetName() {
        return this.storageDataSetName;
    }

    public void setStorageDataSetName(final String storageDataSetName) {
        this.storageDataSetName = storageDataSetName;
    }

    public String getStorageDataSetDescription() {
        return this.storageDataSetDescription;
    }

    public void setStorageDataSetDescription(final String storageDataSetDescription) {
        this.storageDataSetDescription = storageDataSetDescription;
    }

    public String getCreatedUser() {
        return this.createdUser;
    }

    public void setCreatedUser(final String createdUser) {
        this.createdUser = createdUser;
    }

    public String getCreatedTimestamp() {
        return this.createdTimestamp;
    }

    public void setCreatedTimestamp(final String createdTimestamp) {
        this.createdTimestamp = createdTimestamp;
    }

    public String getUpdatedUser() {
        return this.updatedUser;
    }

    public void setUpdatedUser(final String updatedUser) {
        this.updatedUser = updatedUser;
    }

    public String getUpdatedTimestamp() {
        return this.updatedTimestamp;
    }

    public void setUpdatedTimestamp(final String updatedTimestamp) {
        this.updatedTimestamp = updatedTimestamp;
    }

    public String getContainerName() {
        return this.containerName;
    }

    public void setContainerName(final String containerName) {
        this.containerName = containerName;
    }

    public String getStorageDatabaseName() {
        return this.storageDatabaseName;
    }

    public void setStorageDatabaseName(final String storageDatabaseName) {
        this.storageDatabaseName = storageDatabaseName;
    }

    public String getIsAutoRegistered() {
        return this.isAutoRegistered;
    }

    public void setIsAutoRegistered(final String isAutoRegistered) {
        this.isAutoRegistered = isAutoRegistered;
    }

    public String get_id() {
        return this._id;
    }

    public void set_id(final String _id) {
        this._id = _id;
    }

    public ElasticSearchDataset() {

    }

    public ElasticSearchDataset(final String _id, final long storageDataSetId, final String storageDataSetName,
            final String storageDataSetAliasName, final String storageDatabaseName,
            final String storageDataSetDescription, final String isActiveYN, final long userId,
            final String createdUser, final String createdTimestamp, final String updatedUser,
            final String updatedTimestamp, final String isAutoRegistered, final long objectSchemaMapId,
            final long storageSystemId, final long zoneId,
            final String isReadCompatible, final String containerName, final String objectName,
            final String isSelfDiscovered, final String isRegistered, final String createdUserOnStore,
            final String createdTimestampOnStore, final List<Schema> objectSchema,
            final List<ObjectAttributeValue> objectAttributeValues, final List<Tag> tags,
            final List<DatasetOwnershipMap> ownerships,
            final List<SourceProviderDatasetMap> objectDescriptions,
            final List<ObjectAttributeKeyValue> customAttributes) {
        this._id = _id;
        this.storageDataSetId = storageDataSetId;
        this.storageDataSetName = storageDataSetName;
        this.storageDataSetAliasName = storageDataSetAliasName;
        this.storageDatabaseName = storageDatabaseName;
        this.storageDataSetDescription = storageDataSetDescription;
        this.isActiveYN = isActiveYN;
        this.userId = userId;
        this.createdUser = createdUser;
        this.createdTimestamp = createdTimestamp;
        this.updatedUser = updatedUser;
        this.updatedTimestamp = updatedTimestamp;
        this.isAutoRegistered = isAutoRegistered;
        this.objectSchemaMapId = objectSchemaMapId;
        this.storageSystemId = storageSystemId;
        this.zoneId = zoneId;
        this.isReadCompatible = isReadCompatible;
        this.containerName = containerName;
        this.objectName = objectName;
        this.isSelfDiscovered = isSelfDiscovered;
        this.isRegistered = isRegistered;
        this.createdUserOnStore = createdUserOnStore;
        this.createdTimestampOnStore = createdTimestampOnStore;
        this.objectSchema = objectSchema;
        this.objectAttributeValues = objectAttributeValues;
        this.tags = tags;
        this.ownerships = ownerships;
        this.objectDescriptions = objectDescriptions;
        this.customAttributeValues = customAttributes;
    }

    public ElasticSearchDataset(final long storageDataSetId, final String storageDataSetName,
            final String storageDataSetAliasName, final String storageDatabaseName,
            final String storageDataSetDescription, final String isActiveYN, final long userId,
            final String createdUser, final String createdTimestamp, final String updatedUser,
            final String updatedTimestamp, final String isAutoRegistered, final long objectSchemaMapId,
            final long storageSystemId, final long zoneId,
            final String isReadCompatible, final String containerName, final String objectName,
            final String isSelfDiscovered, final String isRegistered, final String createdUserOnStore,
            final String createdTimestampOnStore, final List<Schema> objectSchema,
            final List<ObjectAttributeValue> objectAttributeValues, final List<Tag> tags,
            final List<DatasetOwnershipMap> ownerships,
            final List<SourceProviderDatasetMap> objectDescriptions,
            final List<ObjectAttributeKeyValue> customAttributes) {
        this.storageDataSetId = storageDataSetId;
        this.storageDataSetName = storageDataSetName;
        this.storageDataSetAliasName = storageDataSetAliasName;
        this.storageDatabaseName = storageDatabaseName;
        this.storageDataSetDescription = storageDataSetDescription;
        this.isActiveYN = isActiveYN;
        this.userId = userId;
        this.createdUser = createdUser;
        this.createdTimestamp = createdTimestamp;
        this.updatedUser = updatedUser;
        this.updatedTimestamp = updatedTimestamp;
        this.isAutoRegistered = isAutoRegistered;
        this.objectSchemaMapId = objectSchemaMapId;
        this.storageSystemId = storageSystemId;
        this.zoneId = zoneId;
        this.isReadCompatible = isReadCompatible;
        this.containerName = containerName;
        this.objectName = objectName;
        this.isSelfDiscovered = isSelfDiscovered;
        this.isRegistered = isRegistered;
        this.createdUserOnStore = createdUserOnStore;
        this.createdTimestampOnStore = createdTimestampOnStore;
        this.objectSchema = objectSchema;
        this.objectAttributeValues = objectAttributeValues;
        this.tags = tags;
        this.objectDescriptions = objectDescriptions;
        this.customAttributeValues = customAttributes;
        this.ownerships = ownerships;

    }
}
