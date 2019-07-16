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
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;
import com.fasterxml.jackson.annotation.JsonIgnore;
import io.swagger.annotations.ApiModelProperty;


@Entity
@Table(name = "pc_dataset_column_description_map")
public class DatasetColumnDescriptionMap implements Serializable {

    private static final long serialVersionUID = 1L;

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @ApiModelProperty(notes = "The database generated dataset bodhi column map ID")
    @Column(name = "bodhi_dataset_column_map_id")
    @NotNull
    private long bodhiDatasetColumnMapId;

    @ApiModelProperty(notes = "Dataset Bodhi Map ID")
    @Column(name = "bodhi_dataset_map_id")
    private long bodhiDatasetMapId;

    @ApiModelProperty(notes = "Column Name")
    @Column(name = "column_name")
    private String columnName;

    @ApiModelProperty(notes = "Column Comment")
    @Column(name = "column_comment")
    private String columnComment;

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

    public String getColumnName() {
        return this.columnName;
    }

    public void setColumnName(final String columnName) {
        this.columnName = columnName;
    }

    public String getColumnComment() {
        return this.columnComment;
    }

    public void setColumnComment(final String columnComment) {
        this.columnComment = columnComment;
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

    public long getBodhiDatasetColumnMapId() {
        return this.bodhiDatasetColumnMapId;
    }

    public void setBodhiDatasetColumnMapId(final long bodhiDatasetColumnMapId) {
        this.bodhiDatasetColumnMapId = bodhiDatasetColumnMapId;
    }

    public long getBodhiDatasetMapId() {
        return this.bodhiDatasetMapId;
    }

    public void setBodhiDatasetMapId(final long bodhiDatasetMapId) {
        this.bodhiDatasetMapId = bodhiDatasetMapId;
    }

    public DatasetColumnDescriptionMap() {

    }

    public DatasetColumnDescriptionMap(final long bodhiDatasetMapId, final String columnName,
            final String columnComment,
            final String createdUser, final String createdTimestamp, final String updatedUser,
            final String updatedTimestamp) {
        this.bodhiDatasetMapId = bodhiDatasetMapId;
        this.columnName = columnName;
        this.columnComment = columnComment;
        this.createdUser = createdUser;
        this.createdTimestamp = createdTimestamp;
        this.updatedUser = updatedUser;
        this.updatedTimestamp = updatedTimestamp;
    }

}
