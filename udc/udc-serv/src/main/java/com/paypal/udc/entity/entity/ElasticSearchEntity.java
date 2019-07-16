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

package com.paypal.udc.entity.entity;

import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;


@Document(indexName = "#{@entityIndexName}", type = "udc")
public class ElasticSearchEntity {

    @Id
    private String _id;
    private long entityId;
    private String entityName;
    private String entityDescription;
    private String isActiveYN;
    private String createdUser;
    private String createdTimestamp;
    private String updatedUser;
    private String updatedTimestamp;

    public ElasticSearchEntity() {

    }

    public ElasticSearchEntity(final long entityId, final String entityName, final String entityDescription,
            final String isActiveYN, final String createdUser, final String createdTimestamp, final String updatedUser,
            final String updatedTimestamp) {
        super();
        this.entityId = entityId;
        this.entityName = entityName;
        this.entityDescription = entityDescription;
        this.isActiveYN = isActiveYN;
        this.createdUser = createdUser;
        this.createdTimestamp = createdTimestamp;
        this.updatedUser = updatedUser;
        this.updatedTimestamp = updatedTimestamp;
    }

    public String get_id() {
        return this._id;
    }

    public void set_id(final String _id) {
        this._id = _id;
    }

    public String getIsActiveYN() {
        return this.isActiveYN;
    }

    public long getEntityId() {
        return this.entityId;
    }

    public void setEntityId(final long entityId) {
        this.entityId = entityId;
    }

    public String getEntityName() {
        return this.entityName;
    }

    public void setEntityName(final String entityName) {
        this.entityName = entityName;
    }

    public String getEntityDescription() {
        return this.entityDescription;
    }

    public void setEntityDescription(final String entityDescription) {
        this.entityDescription = entityDescription;
    }

    public void setIsActiveYN(final String isActiveYN) {
        this.isActiveYN = isActiveYN;
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

}
