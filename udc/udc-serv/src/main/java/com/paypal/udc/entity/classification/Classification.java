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

import java.util.List;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Transient;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import io.swagger.annotations.ApiModelProperty;
import java.io.Serializable;


@Entity
@Table(name = "pc_classifications")
public class Classification implements Serializable {

    private static final long serialVersionUID = 1L;

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @ApiModelProperty(notes = "The database generated dataset classification ID")
    @Column(name = "dataset_classification_id")
    @NotNull
    private long datasetClassificationId;

    @ApiModelProperty(notes = "Object Name")
    @Column(name = "object_name")
    @Size(min = 1, message = "name need to have atleast 1 character")
    private String objectName;

    @ApiModelProperty(notes = "Column Name")
    @Column(name = "column_name")
    private String columnName;

    @ApiModelProperty(notes = "Provider ID")
    @Column(name = "provider_id")
    private long providerId;

    @ApiModelProperty(notes = "Classification ID")
    @Column(name = "classification_id")
    private long classificationId;

    @ApiModelProperty(notes = "Classification Comment")
    @Column(name = "classification_comment")
    private String classificationComment;

    @ApiModelProperty(notes = "Data source")
    @Column(name = "data_source")
    private String dataSource;

    @ApiModelProperty(notes = "Data source Type")
    @Column(name = "data_source_type")
    private String dataSourceType;

    @ApiModelProperty(notes = "Container Name")
    @Column(name = "container_name")
    private String containerName;

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

    // @ApiModelProperty(notes = "Misc Attributes")
    // @Column(name = "misc_attributes")
    // private String miscAttributes;

    @Transient
    private List<Long> datasetIds;

    @Transient
    private String providerName;

    public long getDatasetClassificationId() {
        return this.datasetClassificationId;
    }

    public void setDatasetClassificationId(final long datasetClassificationId) {
        this.datasetClassificationId = datasetClassificationId;
    }

    public String getObjectName() {
        return this.objectName;
    }

    public void setObjectName(final String objectName) {
        this.objectName = objectName;
    }

    public String getColumnName() {
        return this.columnName;
    }

    public void setColumnName(final String columnName) {
        this.columnName = columnName;
    }

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

    public long getClassificationId() {
        return this.classificationId;
    }

    public void setClassificationId(final long classificationId) {
        this.classificationId = classificationId;
    }

    public String getClassificationComment() {
        return this.classificationComment;
    }

    public void setClassificationComment(final String classificationComment) {
        this.classificationComment = classificationComment;
    }

    public String getDataSource() {
        return this.dataSource;
    }

    public void setDataSource(final String dataSource) {
        this.dataSource = dataSource;
    }

    public String getDataSourceType() {
        return this.dataSourceType;
    }

    public void setDataSourceType(final String dataSourceType) {
        this.dataSourceType = dataSourceType;
    }

    public String getContainerName() {
        return this.containerName;
    }

    public void setContainerName(final String containerName) {
        this.containerName = containerName;
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

    public List<Long> getDatasetIds() {
        return this.datasetIds;
    }

    public void setDatasetIds(final List<Long> datasetIds) {
        this.datasetIds = datasetIds;
    }

    public Classification() {

    }

    public Classification(final String objectName, final String columnName,
            final long providerId, final long classificationId, final String classificationComment,
            final String dataSource, final String dataSourceType, final String containerName,
            final String createdUser, final String createdTimestamp, final String updatedUser,
            final String updatedTimestamp) {
        this.objectName = objectName;
        this.columnName = columnName;
        this.providerId = providerId;
        this.classificationId = classificationId;
        this.classificationComment = classificationComment;
        this.dataSource = dataSource;
        this.dataSourceType = dataSourceType;
        this.containerName = containerName;
        this.createdUser = createdUser;
        this.createdTimestamp = createdTimestamp;
        this.updatedUser = updatedUser;
        this.updatedTimestamp = updatedTimestamp;
    }

    public Classification(final long datasetClassificationId, final String objectName, final String columnName,
            final long providerId, final long classificationId,
            final String classificationComment, final String dataSource, final String dataSourceType,
            final String containerName, final String createdUser, final String createdTimestamp,
            final String updatedUser, final String updatedTimestamp) {
        this.datasetClassificationId = datasetClassificationId;
        this.objectName = objectName;
        this.columnName = columnName;
        this.providerId = providerId;
        this.classificationId = classificationId;
        this.classificationComment = classificationComment;
        this.dataSource = dataSource;
        this.dataSourceType = dataSourceType;
        this.containerName = containerName;
        this.createdUser = createdUser;
        this.createdTimestamp = createdTimestamp;
        this.updatedUser = updatedUser;
        this.updatedTimestamp = updatedTimestamp;
    }

}
