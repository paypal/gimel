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

package com.paypal.udc.entity.objectschema;

import java.util.List;


public class CollectiveObjectSchemaMap {

    private long objectId;
    private String objectName;
    private String containerName;
    private long storageSystemId;
    private List<Long> clusterIds;
    private String query;
    private List<Schema> objectSchema;
    private List<CollectiveObjectAttributeValue> objectAttributes;
    private String isActiveYN;
    private String createdTimestampOnStore;
    private String createdUserOnStore;
    private String isSelfDiscovered;

    public String getCreatedTimestampOnStore() {
        return this.createdTimestampOnStore;
    }

    public void setCreatedTimestampOnStore(final String createdTimestampOnStore) {
        this.createdTimestampOnStore = createdTimestampOnStore;
    }

    public String getCreatedUserOnStore() {
        return this.createdUserOnStore;
    }

    public void setCreatedUserOnStore(final String createdUserOnStore) {
        this.createdUserOnStore = createdUserOnStore;
    }

    public String getIsActiveYN() {
        return this.isActiveYN;
    }

    public void setIsActiveYN(final String isActiveYN) {
        this.isActiveYN = isActiveYN;
    }

    public long getObjectId() {
        return this.objectId;
    }

    public void setObjectId(final long objectId) {
        this.objectId = objectId;
    }

    public String getObjectName() {
        return this.objectName;
    }

    public void setObjectName(final String objectName) {
        this.objectName = objectName;
    }

    public String getContainerName() {
        return this.containerName;
    }

    public void setContainerName(final String containerName) {
        this.containerName = containerName;
    }

    public long getStorageSystemId() {
        return this.storageSystemId;
    }

    public void setStorageSystemId(final long storageSystemId) {
        this.storageSystemId = storageSystemId;
    }

    public String getIsSelfDiscovered() {
        return this.isSelfDiscovered;
    }

    public void setIsSelfDiscovered(final String isSelfDiscovered) {
        this.isSelfDiscovered = isSelfDiscovered;
    }

    public CollectiveObjectSchemaMap(final long objectId, final String objectName, final String containerName,
            final long storageSystemId, final List<Long> clusterIds, final String query,
            final List<Schema> objectSchema,
            final List<CollectiveObjectAttributeValue> objectAttributes, final String isActiveYN,
            final String createdUserOnStore, final String createdTimestampOnStore, final String isSelfDiscovered) {
        this.objectId = objectId;
        this.objectName = objectName;
        this.containerName = containerName;
        this.storageSystemId = storageSystemId;
        this.clusterIds = clusterIds;
        this.query = query;
        this.objectSchema = objectSchema;
        this.objectAttributes = objectAttributes;
        this.isActiveYN = isActiveYN;
        this.createdUserOnStore = createdUserOnStore;
        this.createdTimestampOnStore = createdTimestampOnStore;
        this.isSelfDiscovered = isSelfDiscovered;
    }

    public List<Long> getClusterIds() {
        return this.clusterIds;
    }

    public void setClusterIds(final List<Long> clusterIds) {
        this.clusterIds = clusterIds;
    }

    public String getQuery() {
        return this.query;
    }

    public void setQuery(final String query) {
        this.query = query;
    }

    public List<Schema> getObjectSchema() {
        return this.objectSchema;
    }

    public void setObjectSchema(final List<Schema> objectSchema) {
        this.objectSchema = objectSchema;
    }

    public List<CollectiveObjectAttributeValue> getObjectAttributes() {
        return this.objectAttributes;
    }

    public void setObjectAttributes(final List<CollectiveObjectAttributeValue> objectAttributes) {
        this.objectAttributes = objectAttributes;
    }

}
