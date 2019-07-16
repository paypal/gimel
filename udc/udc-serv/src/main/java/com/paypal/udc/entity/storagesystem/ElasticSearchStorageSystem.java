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

package com.paypal.udc.entity.storagesystem;

import java.util.List;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;


@Document(indexName = "#{@systemIndexName}", type = "udc")
public class ElasticSearchStorageSystem {

    @Id
    private String _id;
    private long storageSystemId;
    private String storageSystemName;
    private String storageSystemDescription;
    private String isActiveYN;
    private long adminUserId;
    private long zoneId;
    private long entityId;
    private String isReadCompatible;
    private long runningClusterId;
    private String createdUser;
    private String createdTimestamp;
    private String updatedUser;
    private String updatedTimestamp;
    private long storageTypeId;
    private List<StorageSystemAttributeValue> systemAttributeValues;

    public String get_id() {
        return this._id;
    }

    public void set_id(final String _id) {
        this._id = _id;
    }

    public long getEntityId() {
        return this.entityId;
    }

    public void setEntityId(final long entityId) {
        this.entityId = entityId;
    }

    public long getStorageSystemId() {
        return this.storageSystemId;
    }

    public void setStorageSystemId(final long storageSystemId) {
        this.storageSystemId = storageSystemId;
    }

    public String getStorageSystemName() {
        return this.storageSystemName;
    }

    public void setStorageSystemName(final String storageSystemName) {
        this.storageSystemName = storageSystemName;
    }

    public String getStorageSystemDescription() {
        return this.storageSystemDescription;
    }

    public void setStorageSystemDescription(final String storageSystemDescription) {
        this.storageSystemDescription = storageSystemDescription;
    }

    public String getIsActiveYN() {
        return this.isActiveYN;
    }

    public void setIsActiveYN(final String isActiveYN) {
        this.isActiveYN = isActiveYN;
    }

    public long getAdminUserId() {
        return this.adminUserId;
    }

    public void setAdminUserId(final long adminUserId) {
        this.adminUserId = adminUserId;
    }

    public long getZoneId() {
        return this.zoneId;
    }

    public void setZoneId(final long zoneId) {
        this.zoneId = zoneId;
    }

    public String getIsReadCompatible() {
        return this.isReadCompatible;
    }

    public void setIsReadCompatible(final String isReadCompatible) {
        this.isReadCompatible = isReadCompatible;
    }

    public long getRunningClusterId() {
        return this.runningClusterId;
    }

    public void setRunningClusterId(final long runningClusterId) {
        this.runningClusterId = runningClusterId;
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

    public long getStorageTypeId() {
        return this.storageTypeId;
    }

    public void setStorageTypeId(final long storageTypeId) {
        this.storageTypeId = storageTypeId;
    }

    public List<StorageSystemAttributeValue> getSystemAttributeValues() {
        return this.systemAttributeValues;
    }

    public void setSystemAttributeValues(final List<StorageSystemAttributeValue> systemAttributeValues) {
        this.systemAttributeValues = systemAttributeValues;
    }

    public ElasticSearchStorageSystem() {

    }

    public ElasticSearchStorageSystem(final long storageSystemId, final String storageSystemName,
            final String storageSystemDescription, final String isActiveYN, final long adminUserId, final long zoneId,
            final String isReadCompatible,
            final long runningClusterId, final String createdUser, final String createdTimestamp,
            final String updatedUser, final String updatedTimestamp, final long storageTypeId,
            final List<StorageSystemAttributeValue> systemAttributeValues, final long entityId) {
        this.storageSystemId = storageSystemId;
        this.storageSystemName = storageSystemName;
        this.storageSystemDescription = storageSystemDescription;
        this.isActiveYN = isActiveYN;
        this.adminUserId = adminUserId;
        this.zoneId = zoneId;
        this.isReadCompatible = isReadCompatible;
        this.runningClusterId = runningClusterId;
        this.createdUser = createdUser;
        this.createdTimestamp = createdTimestamp;
        this.updatedUser = updatedUser;
        this.updatedTimestamp = updatedTimestamp;
        this.storageTypeId = storageTypeId;
        this.systemAttributeValues = systemAttributeValues;
        this.entityId = entityId;
    }

}
