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

package com.paypal.udc.entity.integration.description;

import java.util.List;
import com.paypal.udc.entity.integration.schema.SchemaDatasetColumnMap;


public class SourceProviderDatasetMap {

    private long datasetId;
    private long providerId;
    private String objectComment;
    private String createdUser;
    private String createdTimestamp;
    private String updatedUser;
    private String updatedTimestamp;
    private List<SchemaDatasetColumnMap> schemaColumnDescriptions;
    private List<DatasetColumnDescriptionMap> miscColumnDescriptions;

    public long getDatasetId() {
        return this.datasetId;
    }

    public void setDatasetId(final long datasetId) {
        this.datasetId = datasetId;
    }

    public List<SchemaDatasetColumnMap> getSchemaColumnDescriptions() {
        return this.schemaColumnDescriptions;
    }

    public void setSchemaColumnDescriptions(final List<SchemaDatasetColumnMap> schemaColumnDescriptions) {
        this.schemaColumnDescriptions = schemaColumnDescriptions;
    }

    public List<DatasetColumnDescriptionMap> getMiscColumnDescriptions() {
        return this.miscColumnDescriptions;
    }

    public void setMiscColumnDescriptions(final List<DatasetColumnDescriptionMap> miscColumnDescriptions) {
        this.miscColumnDescriptions = miscColumnDescriptions;
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

    public String getObjectComment() {
        return this.objectComment;
    }

    public void setObjectComment(final String objectComment) {
        this.objectComment = objectComment;
    }

    public String getCreatedUser() {
        return this.createdUser;
    }

    public void setCreatedUser(final String createdUser) {
        this.createdUser = createdUser;
    }

    public long getProviderId() {
        return this.providerId;
    }

    public void setProviderId(final long providerId) {
        this.providerId = providerId;
    }

    public SourceProviderDatasetMap() {
    }

    public SourceProviderDatasetMap(final long datasetId, final long providerId, final String objectComment,
            final String createdUser, final String createdTimestamp, final String updatedUser,
            final String updatedTimestamp, final List<SchemaDatasetColumnMap> schemaColumnDescriptions,
            final List<DatasetColumnDescriptionMap> miscColumnDescriptions) {
        this.datasetId = datasetId;
        this.providerId = providerId;
        this.objectComment = objectComment;
        this.createdUser = createdUser;
        this.createdTimestamp = createdTimestamp;
        this.updatedUser = updatedUser;
        this.updatedTimestamp = updatedTimestamp;
        this.schemaColumnDescriptions = schemaColumnDescriptions;
        this.miscColumnDescriptions = miscColumnDescriptions;
    }

}
