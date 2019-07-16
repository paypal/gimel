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

package com.paypal.udc.entity.rangerpolicy;

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
import javax.validation.constraints.Size;
import com.fasterxml.jackson.annotation.JsonIgnore;
import io.swagger.annotations.ApiModelProperty;


@Entity
@Table(name = "pc_ranger_policy")
public class DerivedPolicy implements Serializable {

    private static final long serialVersionUID = 1L;

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @ApiModelProperty(notes = "The ranger generated auto policy ID")
    @Column(name = "derived_policy_id")
    @NotNull
    private long derivedPolicyId;

    @ApiModelProperty(notes = "Policy ID")
    @Column(name = "policy_id")
    @NotNull
    private int policyId;

    @ApiModelProperty(notes = "Cluster ID")
    @Column(name = "cluster_id")
    @NotNull
    private long clusterId;

    @ApiModelProperty(notes = "Name of the Policy")
    @Column(name = "policy_name")
    @Size(min = 1, message = "name need to have atleast 1 character")
    @NotNull
    private String policyName;

    @ApiModelProperty(notes = "Policy Type")
    @Column(name = "type_name")
    @Size(min = 1, message = "name need to have atleast 1 character")
    @NotNull
    private String typeName;

    @ApiModelProperty(notes = "Policy applied locations")
    @Column(name = "policy_locations")
    private String policyLocations;

    @ApiModelProperty(notes = "Policy on Column")
    @Column(name = "`column`")
    private String column;

    @ApiModelProperty(notes = "Policy on column family")
    @Column(name = "column_family")
    private String columnFamily;

    @ApiModelProperty(notes = "Policy on database")
    @Column(name = "`database`")
    private String database;

    @ApiModelProperty(notes = "Policy on table")
    @Column(name = "`table`")
    private String table;

    @ApiModelProperty(notes = "Policy on queue")
    @Column(name = "`queue`")
    private String queue;

    @ApiModelProperty(notes = "Is the Storage Category active ?")
    @Column(name = "is_active_y_n")
    private String isActiveYN;

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
    private List<DerivedPolicyItem> policyItems;

    public long getClusterId() {
        return this.clusterId;
    }

    public void setClusterId(final long clusterId) {
        this.clusterId = clusterId;
    }

    public long getDerivedPolicyId() {
        return this.derivedPolicyId;
    }

    public void setDerivedPolicyId(final long derivedPolicyId) {
        this.derivedPolicyId = derivedPolicyId;
    }

    public int getPolicyId() {
        return this.policyId;
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

    public void setPolicyId(final int policyId) {
        this.policyId = policyId;
    }

    public String getPolicyName() {
        return this.policyName;
    }

    public void setPolicyName(final String policyName) {
        this.policyName = policyName;
    }

    public String getTypeName() {
        return this.typeName;
    }

    public void setTypeName(final String typeName) {
        this.typeName = typeName;
    }

    public String getPolicyLocations() {
        return this.policyLocations == null ? "" : this.policyLocations;
    }

    public void setPolicyLocations(final String policyLocations) {
        this.policyLocations = policyLocations;
    }

    public List<DerivedPolicyItem> getPolicyItems() {
        return this.policyItems;
    }

    public void setPolicyItems(final List<DerivedPolicyItem> policyItems) {
        this.policyItems = policyItems;
    }

    public String getColumn() {
        return this.column == null ? "" : this.column;
    }

    public void setColumn(final String column) {
        this.column = column;
    }

    public String getColumnFamily() {
        return this.columnFamily == null ? "" : this.columnFamily;
    }

    public void setColumnFamily(final String columnFamily) {
        this.columnFamily = columnFamily;
    }

    public String getDatabase() {
        return this.database == null ? "" : this.database;
    }

    public void setDatabase(final String database) {
        this.database = database;
    }

    public String getTable() {
        return this.table == null ? "" : this.table;
    }

    public void setTable(final String table) {
        this.table = table;
    }

    public String getQueue() {
        return this.queue == null ? "" : this.queue;
    }

    public void setQueue(final String queue) {
        this.queue = queue;
    }

    public DerivedPolicy() {

    }

    public DerivedPolicy(final long derivedPolicyId, final int policyId, final long clusterId, final String policyName,
            final String typeName, final String policyLocations, final String column, final String columnFamily,
            final String database, final String table, final String queue, final String isActiveYN,
            final String createdUser, final String createdTimestamp, final String updatedUser,
            final String updatedTimestamp) {
        this.derivedPolicyId = derivedPolicyId;
        this.policyId = policyId;
        this.clusterId = clusterId;
        this.policyName = policyName;
        this.typeName = typeName;
        this.policyLocations = policyLocations;
        this.column = column;
        this.columnFamily = columnFamily;
        this.database = database;
        this.table = table;
        this.queue = queue;
        this.isActiveYN = isActiveYN;
        this.createdUser = createdUser;
        this.createdTimestamp = createdTimestamp;
        this.updatedUser = updatedUser;
        this.updatedTimestamp = updatedTimestamp;
    }

    public DerivedPolicy(final int policyId, final long clusterId, final String policyName, final String typeName,
            final String policyLocations, final String column, final String columnFamily,
            final String database, final String table, final String queue, final String isActiveYN,
            final String createdUser, final String createdTimestamp, final String updatedUser,
            final String updatedTimestamp) {
        this.policyId = policyId;
        this.clusterId = clusterId;
        this.policyName = policyName;
        this.typeName = typeName;
        this.policyLocations = policyLocations;
        this.column = column;
        this.columnFamily = columnFamily;
        this.database = database;
        this.table = table;
        this.queue = queue;
        this.isActiveYN = isActiveYN;
        this.createdUser = createdUser;
        this.createdTimestamp = createdTimestamp;
        this.updatedUser = updatedUser;
        this.updatedTimestamp = updatedTimestamp;
    }

}
