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

package com.paypal.udc.entity.storagetype;

import java.util.List;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;


@Document(indexName = "#{@typeIndexName}", type = "udc")
public class ElasticSearchStorageType {

    @Id
    private String _id;
    private long storageTypeId;
    private String storageTypeName;
    private String storageTypeDescription;
    private String createdUser;
    private String createdTimestamp;
    private String updatedUser;
    private String updatedTimestamp;
    private long storageId;
    private String isActiveYN;
    private List<StorageTypeAttributeKey> attributeKeys;

    public ElasticSearchStorageType() {

    }

    public ElasticSearchStorageType(final long storageTypeId, final String storageTypeName,
            final String storageTypeDescription, final String createdUser, final String createdTimestamp,
            final String updatedUser, final String updatedTimestamp, final long storageId,
            final String isActiveYN, final List<StorageTypeAttributeKey> attributeKeys) {
        this.storageTypeId = storageTypeId;
        this.storageTypeName = storageTypeName;
        this.storageTypeDescription = storageTypeDescription;
        this.createdUser = createdUser;
        this.createdTimestamp = createdTimestamp;
        this.updatedUser = updatedUser;
        this.updatedTimestamp = updatedTimestamp;
        this.storageId = storageId;
        this.isActiveYN = isActiveYN;
        this.attributeKeys = attributeKeys;
    }

    public String get_id() {
        return this._id;
    }

    public void set_id(final String _id) {
        this._id = _id;
    }

    public long getStorageTypeId() {
        return this.storageTypeId;
    }

    public void setStorageTypeId(final long storageTypeId) {
        this.storageTypeId = storageTypeId;
    }

    public String getStorageTypeName() {
        return this.storageTypeName;
    }

    public void setStorageTypeName(final String storageTypeName) {
        this.storageTypeName = storageTypeName;
    }

    public String getStorageTypeDescription() {
        return this.storageTypeDescription;
    }

    public void setStorageTypeDescription(final String storageTypeDescription) {
        this.storageTypeDescription = storageTypeDescription;
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

    public long getStorageId() {
        return this.storageId;
    }

    public void setStorageId(final long storageId) {
        this.storageId = storageId;
    }

    public String getIsActiveYN() {
        return this.isActiveYN;
    }

    public void setIsActiveYN(final String isActiveYN) {
        this.isActiveYN = isActiveYN;
    }

    public List<StorageTypeAttributeKey> getAttributeKeys() {
        return this.attributeKeys;
    }

    public void setAttributeKeys(final List<StorageTypeAttributeKey> attributeKeys) {
        this.attributeKeys = attributeKeys;
    }

}
