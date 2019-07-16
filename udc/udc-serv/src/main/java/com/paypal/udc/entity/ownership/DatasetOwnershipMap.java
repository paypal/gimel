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

package com.paypal.udc.entity.ownership;

import java.io.Serializable;
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


@Entity
@Table(name = "pc_dataset_ownership_map")
public class DatasetOwnershipMap implements Serializable {

    private static final long serialVersionUID = 1L;

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @ApiModelProperty(notes = "The database generated dataset ID")
    @Column(name = "dataset_ownership_map_id")
    @NotNull
    private long datasetOwnershipMapId;

    @ApiModelProperty(notes = "Dataset ID")
    @Column(name = "storage_dataset_id")
    @NotNull
    private long storageDatasetId;

    @ApiModelProperty(notes = "Provider ID")
    @Column(name = "provider_id")
    @NotNull
    private long providerId;

    @ApiModelProperty(notes = "System ID")
    @Column(name = "storage_system_id")
    @NotNull
    private long storageSystemId;

    @ApiModelProperty(notes = "Owner Name")
    @Column(name = "owner_name")
    @NotNull
    @Size(min = 1, message = "name need to have atleast 1 character")
    private String ownerName;

    @ApiModelProperty(notes = "Container Name")
    @Column(name = "container_name")
    @NotNull
    private String containerName;

    @ApiModelProperty(notes = "Object Name")
    @Column(name = "object_name")
    @NotNull
    private String objectName;

    @ApiModelProperty(notes = "Ownership Comment")
    @Column(name = "ownership_comment")
    @NotNull
    private String ownershipComment;

    @ApiModelProperty(notes = "Owner email")
    @Column(name = "owner_email")
    @NotNull
    private String ownerEmail;

    @ApiModelProperty(notes = "Email IList")
    @Column(name = "email_ilist")
    @NotNull
    private String emailIlist;

    @ApiModelProperty(notes = "Claimed By")
    @Column(name = "claimed_by")
    private String claimedBy;

    @ApiModelProperty(notes = "Is the Email Notified")
    @Column(name = "is_notified")
    private String isNotified;

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

    @Transient
    private String providerName;

    @Transient
    private String storageSystemName;

    @Transient
    private String otherOwners;

    public String getIsNotified() {
        return this.isNotified;
    }

    public void setIsNotified(final String isNotified) {
        this.isNotified = isNotified;
    }

    public String getOtherOwners() {
        return this.otherOwners;
    }

    public void setOtherOwners(final String otherOwners) {
        this.otherOwners = otherOwners;
    }

    public String getClaimedBy() {
        return this.claimedBy;
    }

    public void setClaimedBy(final String claimedBy) {
        this.claimedBy = claimedBy;
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

    public DatasetOwnershipMap() {
    }

    public DatasetOwnershipMap(@NotNull final long datasetOwnershipMapId, @NotNull final long storageDatasetId,
            @NotNull final long providerId, @NotNull final long storageSystemId, @NotNull final String ownerName,
            @NotNull final String containerName, @NotNull final String objectName,
            @NotNull final String ownershipComment, @NotNull final String ownerEmail, @NotNull final String emailIlist,
            final String createdUser, final String createdTimestamp, final String updatedUser,
            final String updatedTimestamp, final String claimedBy, @NotNull final String isNotified) {
        this.datasetOwnershipMapId = datasetOwnershipMapId;
        this.storageDatasetId = storageDatasetId;
        this.providerId = providerId;
        this.storageSystemId = storageSystemId;
        this.ownerName = ownerName;
        this.containerName = containerName;
        this.objectName = objectName;
        this.ownershipComment = ownershipComment;
        this.ownerEmail = ownerEmail;
        this.emailIlist = emailIlist;
        this.createdUser = createdUser;
        this.createdTimestamp = createdTimestamp;
        this.updatedUser = updatedUser;
        this.updatedTimestamp = updatedTimestamp;
        this.claimedBy = claimedBy;
        this.isNotified = isNotified;
    }

    public DatasetOwnershipMap(@NotNull final long storageDatasetId, @NotNull final long providerId,
            @NotNull final long storageSystemId, @NotNull final String ownerName, @NotNull final String containerName,
            @NotNull final String objectName, @NotNull final String ownershipComment, @NotNull final String ownerEmail,
            @NotNull final String emailIlist, final String createdUser, final String createdTimestamp,
            final String updatedUser, final String updatedTimestamp, final String claimedBy,
            @NotNull final String isNotified) {
        this.storageDatasetId = storageDatasetId;
        this.providerId = providerId;
        this.storageSystemId = storageSystemId;
        this.ownerName = ownerName;
        this.containerName = containerName;
        this.objectName = objectName;
        this.ownershipComment = ownershipComment;
        this.ownerEmail = ownerEmail;
        this.emailIlist = emailIlist;
        this.createdUser = createdUser;
        this.createdTimestamp = createdTimestamp;
        this.updatedUser = updatedUser;
        this.updatedTimestamp = updatedTimestamp;
        this.claimedBy = claimedBy;
        this.isNotified = isNotified;
    }

    public long getDatasetOwnershipMapId() {
        return this.datasetOwnershipMapId;
    }

    public void setDatasetOwnershipMapId(final long datasetOwnershipMapId) {
        this.datasetOwnershipMapId = datasetOwnershipMapId;
    }

    public long getStorageDatasetId() {
        return this.storageDatasetId;
    }

    public void setStorageDatasetId(final long storageDatasetId) {
        this.storageDatasetId = storageDatasetId;
    }

    public long getProviderId() {
        return this.providerId;
    }

    public void setProviderId(final long providerId) {
        this.providerId = providerId;
    }

    public long getStorageSystemId() {
        return this.storageSystemId;
    }

    public void setStorageSystemId(final long storageSystemId) {
        this.storageSystemId = storageSystemId;
    }

    public String getOwnerName() {
        return this.ownerName;
    }

    public void setOwnerName(final String ownerName) {
        this.ownerName = ownerName;
    }

    public String getContainerName() {
        return this.containerName;
    }

    public void setContainerName(final String containerName) {
        this.containerName = containerName;
    }

    public String getObjectName() {
        return this.objectName;
    }

    public void setObjectName(final String objectName) {
        this.objectName = objectName;
    }

    public String getOwnershipComment() {
        return this.ownershipComment;
    }

    public void setOwnershipComment(final String ownershipComment) {
        this.ownershipComment = ownershipComment;
    }

    public String getOwnerEmail() {
        return this.ownerEmail;
    }

    public void setOwnerEmail(final String ownerEmail) {
        this.ownerEmail = ownerEmail;
    }

    public String getEmailIlist() {
        return this.emailIlist;
    }

    public void setEmailIlist(final String emailIlist) {
        this.emailIlist = emailIlist;
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
