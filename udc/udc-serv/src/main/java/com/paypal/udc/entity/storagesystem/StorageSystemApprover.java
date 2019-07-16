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

package com.paypal.udc.entity.storagesystem;

import io.swagger.annotations.ApiModelProperty;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.IdClass;
import javax.persistence.Table;


@Entity
@Table(name = "pc_storage_system_approver")
@IdClass(StorageSystemApproverPK.class)
public class StorageSystemApprover {

    @Id
    @Column(name = "approver_id")
    @ApiModelProperty(notes = "Approver ID")
    private long approverId;

    @Id
    @Column(name = "storage_system_id")
    @ApiModelProperty(notes = "The database generated storage system ID")
    private long storageSystemId;

    @Column(name = "cre_user")
    @ApiModelProperty(notes = "Created User")
    private String createdUser;

    @Column(name = "cre_ts")
    @ApiModelProperty(notes = "Created Timestamp")
    private String createdTimestamp;

    @Column(name = "upd_user")
    @ApiModelProperty(notes = "Updated User")
    private String updatedUser;

    @Column(name = "upd_ts")
    @ApiModelProperty(notes = "Updated Timestamp")
    private String updatedTimestamp;

    public long getApproverId() {
        return approverId;
    }

    public void setApproverId(long approverId) {
        this.approverId = approverId;
    }

    public long getStorageSystemId() {
        return storageSystemId;
    }

    public void setStorageSystemId(long storageSystemId) {
        this.storageSystemId = storageSystemId;
    }

    public String getCreatedUser() {
        return createdUser;
    }

    public void setCreatedUser(String createdUser) {
        this.createdUser = createdUser;
    }

    public String getCreatedTimestamp() {
        return createdTimestamp;
    }

    public void setCreatedTimestamp(String createdTimestamp) {
        this.createdTimestamp = createdTimestamp;
    }

    public String getUpdatedUser() {
        return updatedUser;
    }

    public void setUpdatedUser(String updatedUser) {
        this.updatedUser = updatedUser;
    }

    public String getUpdatedTimestamp() {
        return updatedTimestamp;
    }

    public void setUpdatedTimestamp(String updatedTimestamp) {
        this.updatedTimestamp = updatedTimestamp;
    }

}
