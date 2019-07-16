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

import java.io.Serializable;
import java.util.List;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Transient;
import javax.validation.constraints.NotNull;
import com.fasterxml.jackson.annotation.JsonIgnore;
import io.swagger.annotations.ApiModelProperty;


@Entity
@Table(name = "pc_dataset_description_map")
public class DatasetDescriptionMap implements Serializable {

    private static final long serialVersionUID = 1L;

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @ApiModelProperty(notes = "The database generated dataset bodhi map ID")
    @Column(name = "bodhi_dataset_map_id")
    @NotNull
    private long bodhiDatasetMapId;

    @ApiModelProperty(notes = "Storage System ID")
    @Column(name = "storage_system_id")
    private long storageSystemId;

    @ApiModelProperty(notes = "provider Id")
    @Column(name = "provider_id")
    private long providerId;

    @ApiModelProperty(notes = "Storage Dataset ID")
    @Column(name = "storage_dataset_id")
    private long storageDatasetId;

    @ApiModelProperty(notes = "Object Name")
    @Column(name = "object_name")
    private String objectName;

    @ApiModelProperty(notes = "Container Name")
    @Column(name = "container_name")
    private String containerName;

    @ApiModelProperty(notes = "Object Comment")
    @Column(name = "object_comment")
    private String objectComment;

    @ApiModelProperty(notes = "Created User")
    @Column(name = "cre_user")
    private String createdUser;

    @ApiModelProperty(notes = "Created Timestamp")
    @Column(name = "cre_ts")
    @JsonIgnore
    private String createdTimestamp;

    @ApiModelProperty(notes = "Updated User")
    @Column(name = "upd_user")
    private String updatedUser;

    @ApiModelProperty(notes = "Updated Timestamp")
    @Column(name = "upd_ts")
    @JsonIgnore
    private String updatedTimestamp;

    @Transient
    private List<DatasetColumnDescriptionMap> columns;

    @Transient
    private String storageSystemName;

    @Transient
    private String providerName;

    public long getProviderId() {
        return this.providerId;
    }

    public void setProviderId(final long providerId) {
        this.providerId = providerId;
    }

    public String getProviderName() {
        return this.providerName;
    }

    public void setProviderName(final String providerName) {
        this.providerName = providerName;
    }

    public String getStorageSystemName() {
        return this.storageSystemName;
    }

    public void setStorageSystemName(final String storageSystemName) {
        this.storageSystemName = storageSystemName;
    }

    public long getBodhiDatasetMapId() {
        return this.bodhiDatasetMapId;
    }

    public void setBodhiDatasetMapId(final long bodhiDatasetMapId) {
        this.bodhiDatasetMapId = bodhiDatasetMapId;
    }

    public long getStorageSystemId() {
        return this.storageSystemId;
    }

    public void setStorageSystemId(final long storageSystemId) {
        this.storageSystemId = storageSystemId;
    }

    public long getStorageDatasetId() {
        return this.storageDatasetId;
    }

    public void setStorageDatasetId(final long storageDatasetId) {
        this.storageDatasetId = storageDatasetId;
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

    public List<DatasetColumnDescriptionMap> getColumns() {
        return this.columns;
    }

    public void setColumns(final List<DatasetColumnDescriptionMap> columns) {
        this.columns = columns;
    }

    public DatasetDescriptionMap() {

    }

    public DatasetDescriptionMap(final long bodhiDatasetMapId, final long storageSystemId, final long providerId,
            final long storageDatasetId, final String objectName, final String containerName,
            final String objectComment, final String createdUser, final String createdTimestamp,
            final String updatedUser, final String updatedTimestamp) {
        this.bodhiDatasetMapId = bodhiDatasetMapId;
        this.storageSystemId = storageSystemId;
        this.storageDatasetId = storageDatasetId;
        this.objectName = objectName;
        this.containerName = containerName;
        this.objectComment = objectComment;
        this.createdUser = createdUser;
        this.createdTimestamp = createdTimestamp;
        this.updatedUser = updatedUser;
        this.updatedTimestamp = updatedTimestamp;
        this.providerId = providerId;
    }

    public DatasetDescriptionMap(final long storageSystemId, final long providerId, final long storageDatasetId,
            final String objectName, final String containerName, final String objectComment, final String createdUser,
            final String createdTimestamp, final String updatedUser, final String updatedTimestamp) {
        this.storageSystemId = storageSystemId;
        this.storageDatasetId = storageDatasetId;
        this.objectName = objectName;
        this.containerName = containerName;
        this.objectComment = objectComment;
        this.createdUser = createdUser;
        this.createdTimestamp = createdTimestamp;
        this.updatedUser = updatedUser;
        this.updatedTimestamp = updatedTimestamp;
        this.providerId = providerId;
    }

    @Override
    public String toString() {
        return "DatasetDescriptionMap [bodhiDatasetMapId=" + this.bodhiDatasetMapId + ", storageSystemId="
                + this.storageSystemId + ", providerId=" + this.providerId + ", storageDatasetId="
                + this.storageDatasetId + ", objectName=" + this.objectName + ", containerName=" + this.containerName
                + ", objectComment=" + this.objectComment + ", createdUser=" + this.createdUser + ", createdTimestamp="
                + this.createdTimestamp + ", updatedUser=" + this.updatedUser + ", updatedTimestamp="
                + this.updatedTimestamp + ", columns=" + this.columns + ", storageSystemName="
                + this.storageSystemName + ", providerName=" + this.providerName + "]";
    }

}
