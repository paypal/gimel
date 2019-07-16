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

package com.paypal.udc.entity.integration.common;

import java.io.Serializable;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModelProperty;


@Entity
@Table(name = "pc_storage_dataset_tag_map")
public class DatasetTagMap implements Serializable {

    private static final long serialVersionUID = 1L;

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @ApiModelProperty(notes = "The database generated dataset tag ID")
    @Column(name = "dataset_tag_id")
    @NotNull
    private long datasetTagId;

    @ApiModelProperty(notes = "Storage Dataset ID")
    @Column(name = "storage_dataset_id")
    private long storageDatasetId;

    @ApiModelProperty(notes = "Tag Id")
    @Column(name = "tag_id")
    private long tagId;

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

    public long getTagId() {
        return this.tagId;
    }

    public void setTagId(final long tagId) {
        this.tagId = tagId;
    }

    public String getCreatedUser() {
        return this.createdUser;
    }

    public void setCreatedUser(final String createdUser) {
        this.createdUser = createdUser;
    }

    @JsonIgnore
    public String getCreatedTimestamp() {
        return this.createdTimestamp;
    }

    @JsonProperty
    public void setCreatedTimestamp(final String createdTimestamp) {
        this.createdTimestamp = createdTimestamp;
    }

    @JsonIgnore
    public String getUpdatedUser() {
        return this.updatedUser;
    }

    @JsonProperty
    public void setUpdatedUser(final String updatedUser) {
        this.updatedUser = updatedUser;
    }

    public String getUpdatedTimestamp() {
        return this.updatedTimestamp;
    }

    public void setUpdatedTimestamp(final String updatedTimestamp) {
        this.updatedTimestamp = updatedTimestamp;
    }

    public DatasetTagMap() {

    }

    public long getDatasetTagId() {
        return this.datasetTagId;
    }

    public void setDatasetTagId(final long datasetTagId) {
        this.datasetTagId = datasetTagId;
    }

    public long getStorageDatasetId() {
        return this.storageDatasetId;
    }

    public void setStorageDatasetId(final long storageDatasetId) {
        this.storageDatasetId = storageDatasetId;
    }

    public DatasetTagMap(final long storageDatasetId, final long tagId, final String createdUser,
            final String createdTimestamp,
            final String updatedUser, final String updatedTimestamp) {
        this.storageDatasetId = storageDatasetId;
        this.tagId = tagId;
        this.createdUser = createdUser;
        this.createdTimestamp = createdTimestamp;
        this.updatedUser = updatedUser;
        this.updatedTimestamp = updatedTimestamp;
    }

    @Override
    public String toString() {
        return "DatasetTagMap [datasetTagId=" + this.datasetTagId + ", storageDatasetId=" + this.storageDatasetId
                + ", tagId=" + this.tagId + ", createdUser=" + this.createdUser + ", createdTimestamp="
                + this.createdTimestamp + ", updatedUser=" + this.updatedUser + ", updatedTimestamp="
                + this.updatedTimestamp + "]";
    }

}
