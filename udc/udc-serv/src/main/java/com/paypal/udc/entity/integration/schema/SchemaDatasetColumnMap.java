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

package com.paypal.udc.entity.integration.schema;

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
@Table(name = "pc_schema_dataset_column_map")
public class SchemaDatasetColumnMap implements Serializable {

    private static final long serialVersionUID = 1L;

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @ApiModelProperty(notes = "The database generated dataset schema column map ID")
    @Column(name = "schema_dataset_column_map_id")
    @NotNull
    private long schemaDatasetColumnMapId;

    @ApiModelProperty(notes = "Dataset Schema Map ID")
    @Column(name = "schema_dataset_map_id")
    private long schemaDatasetMapId;

    @ApiModelProperty(notes = "Column Code")
    @Column(name = "column_code")
    private String columnCode;

    @ApiModelProperty(notes = "Column Name")
    @Column(name = "column_name")
    private String columnName;

    @ApiModelProperty(notes = "Column Datatype")
    @Column(name = "column_datatype")
    private String columnDatatype;

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

    public long getSchemaDatasetColumnMapId() {
        return this.schemaDatasetColumnMapId;
    }

    public void setSchemaDatasetColumnMapId(final long schemaDatasetColumnMapId) {
        this.schemaDatasetColumnMapId = schemaDatasetColumnMapId;
    }

    public long getSchemaDatasetMapId() {
        return this.schemaDatasetMapId;
    }

    public void setSchemaDatasetMapId(final long schemaDatasetMapId) {
        this.schemaDatasetMapId = schemaDatasetMapId;
    }

    public String getColumnCode() {
        return this.columnCode;
    }

    public void setColumnCode(final String columnCode) {
        this.columnCode = columnCode;
    }

    public String getColumnName() {
        return this.columnName;
    }

    public void setColumnName(final String columnName) {
        this.columnName = columnName;
    }

    public String getColumnDatatype() {
        return this.columnDatatype;
    }

    public void setColumnDatatype(final String columnDatatype) {
        this.columnDatatype = columnDatatype;
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

    public SchemaDatasetColumnMap(final long schemaDatasetMapId, final String columnCode, final String columnName,
            final String columnDatatype, final String columnComment, final String createdUser,
            final String createdTimestamp, final String updatedUser, final String updatedTimestamp) {
        this.schemaDatasetMapId = schemaDatasetMapId;
        this.columnCode = columnCode;
        this.columnName = columnName;
        this.columnDatatype = columnDatatype;
        this.columnComment = columnComment;
        this.createdUser = createdUser;
        this.createdTimestamp = createdTimestamp;
        this.updatedUser = updatedUser;
        this.updatedTimestamp = updatedTimestamp;
    }

    public SchemaDatasetColumnMap() {

    }
}
