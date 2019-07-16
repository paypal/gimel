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

package com.paypal.udc.entity.cluster;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModelProperty;


@Entity
@Table(name = "pc_storage_cluster_attribute_value")
public class ClusterAttributeValue {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @ApiModelProperty(notes = "The database generated storage cluster attribute value ID")
    @Column(name = "cluster_attribute_value_id")
    @NotNull
    private long clusterAttributeValueId;

    @ApiModelProperty(notes = "The database generated storage cluster attribute key ID")
    @Column(name = "cluster_attribute_key_id")
    @NotNull
    private long clusterAttributeKeyId;

    @ApiModelProperty(notes = "The database generated storage cluster ID")
    @Column(name = "cluster_id")
    @NotNull
    private long clusterId;

    @ApiModelProperty(notes = "Storage Cluster Attribute Value")
    @Column(name = "cluster_attribute_value")
    @Size(min = 1, message = "Name need to have atleast 1 character")
    @NotNull
    private String clusterAttributeValue;

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

    public long getClusterAttributeKeyId() {
        return this.clusterAttributeKeyId;
    }

    public void setClusterAttributeKeyId(final long clusterAttributeKeyId) {
        this.clusterAttributeKeyId = clusterAttributeKeyId;
    }

    public long getClusterAttributeValueId() {
        return this.clusterAttributeValueId;
    }

    public void setClusterAttributeValueId(final long clusterAttributeValueId) {
        this.clusterAttributeValueId = clusterAttributeValueId;
    }

    public long getClusterId() {
        return this.clusterId;
    }

    public void setClusterId(final long clusterId) {
        this.clusterId = clusterId;
    }

    public String getClusterAttributeValue() {
        return this.clusterAttributeValue;
    }

    public void setClusterAttributeValue(final String clusterAttributeValue) {
        this.clusterAttributeValue = clusterAttributeValue;
    }

    ClusterAttributeValue() {

    }

    public ClusterAttributeValue(final long clusterAttributeKeyId, final long clusterId,
            final String clusterAttributeValue, final String createdUser, final String createdTimestamp,
            final String updatedUser, final String updatedTimestamp) {
        this.clusterAttributeKeyId = clusterAttributeKeyId;
        this.clusterId = clusterId;
        this.clusterAttributeValue = clusterAttributeValue;
        this.createdUser = createdUser;
        this.createdTimestamp = createdTimestamp;
        this.updatedUser = updatedUser;
        this.updatedTimestamp = updatedTimestamp;
    }

    public ClusterAttributeValue(final long clusterAttributeValueId, final long clusterAttributeKeyId,
            final long clusterId, final String clusterAttributeValue, final String createdUser,
            final String createdTimestamp, final String updatedUser, final String updatedTimestamp) {
        this.clusterAttributeValueId = clusterAttributeValueId;
        this.clusterAttributeKeyId = clusterAttributeKeyId;
        this.clusterId = clusterId;
        this.clusterAttributeValue = clusterAttributeValue;
        this.createdUser = createdUser;
        this.createdTimestamp = createdTimestamp;
        this.updatedUser = updatedUser;
        this.updatedTimestamp = updatedTimestamp;
    }

}
