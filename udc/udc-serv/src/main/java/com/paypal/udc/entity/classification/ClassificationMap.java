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

package com.paypal.udc.entity.classification;

import java.io.Serializable;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;
import io.swagger.annotations.ApiModelProperty;


@Entity
@Table(name = "pc_classifications_map")
public class ClassificationMap implements Serializable {

    private static final long serialVersionUID = 1L;

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @ApiModelProperty(notes = "The database generated dataset classification map ID")
    @Column(name = "dataset_classification_map_id")
    @NotNull
    private long datasetClassificationMapId;

    @ApiModelProperty(notes = "The database generated dataset classification ID")
    @Column(name = "dataset_classification_id")
    @NotNull
    private long datasetClassificationId;

    @ApiModelProperty(notes = "The database generated dataset ID")
    @Column(name = "storage_dataset_id")
    @NotNull
    private long storageDataSetId;

    @ApiModelProperty(notes = "The database generated zone ID")
    @Column(name = "zone_id")
    @NotNull
    private long zoneId;

    @ApiModelProperty(notes = "The database generated entity ID")
    @Column(name = "entity_id")
    @NotNull
    private long entityId;

    @ApiModelProperty(notes = "The database generated system ID")
    @Column(name = "storage_system_id")
    @NotNull
    private long storageSystemId;

    @ApiModelProperty(notes = "Is the classification active ?")
    @Column(name = "is_active_y_n")
    private String isActiveYN;

    @ApiModelProperty(notes = "Created User")
    @Column(name = "cre_user")
    private String createdUser;

    @ApiModelProperty(notes = "Created Timestamp")
    @Column(name = "cre_ts")
    private String createdTimestamp;

    @ApiModelProperty(notes = "Updated User")
    @Column(name = "upd_user")
    private String updatedUser;

    @ApiModelProperty(notes = "Updated Timestamp")
    @Column(name = "upd_ts")
    private String updatedTimestamp;

    public long getDatasetClassificationMapId() {
        return this.datasetClassificationMapId;
    }

    public void setDatasetClassificationMapId(final long datasetClassificationMapId) {
        this.datasetClassificationMapId = datasetClassificationMapId;
    }

    public long getDatasetClassificationId() {
        return this.datasetClassificationId;
    }

    public void setDatasetClassificationId(final long datasetClassificationId) {
        this.datasetClassificationId = datasetClassificationId;
    }

    public long getStorageDataSetId() {
        return this.storageDataSetId;
    }

    public void setStorageDataSetId(final long storageDataSetId) {
        this.storageDataSetId = storageDataSetId;
    }

    public String getIsActiveYN() {
        return this.isActiveYN;
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

    public long getZoneId() {
        return this.zoneId;
    }

    public void setZoneId(final long zoneId) {
        this.zoneId = zoneId;
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

    public ClassificationMap() {

    }

    public ClassificationMap(@NotNull final long datasetClassificationMapId,
            @NotNull final long datasetClassificationId, @NotNull final long storageDataSetId,
            @NotNull final long zoneId, @NotNull final long entityId, @NotNull final long storageSystemId,
            final String isActiveYN, final String createdUser, final String createdTimestamp, final String updatedUser,
            final String updatedTimestamp) {
        this.datasetClassificationMapId = datasetClassificationMapId;
        this.datasetClassificationId = datasetClassificationId;
        this.storageDataSetId = storageDataSetId;
        this.zoneId = zoneId;
        this.entityId = entityId;
        this.storageSystemId = storageSystemId;
        this.isActiveYN = isActiveYN;
        this.createdUser = createdUser;
        this.createdTimestamp = createdTimestamp;
        this.updatedUser = updatedUser;
        this.updatedTimestamp = updatedTimestamp;
    }

    public ClassificationMap(@NotNull final long datasetClassificationId, @NotNull final long storageDataSetId,
            @NotNull final long zoneId, @NotNull final long entityId, @NotNull final long storageSystemId,
            final String isActiveYN, final String createdUser, final String createdTimestamp, final String updatedUser,
            final String updatedTimestamp) {
        this.datasetClassificationId = datasetClassificationId;
        this.storageDataSetId = storageDataSetId;
        this.zoneId = zoneId;
        this.entityId = entityId;
        this.storageSystemId = storageSystemId;
        this.isActiveYN = isActiveYN;
        this.createdUser = createdUser;
        this.createdTimestamp = createdTimestamp;
        this.updatedUser = updatedUser;
        this.updatedTimestamp = updatedTimestamp;
    }

}
