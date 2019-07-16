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
@Table(name = "pc_storage_system_discovery")
public class StorageSystemDiscovery implements Serializable {

    private static final long serialVersionUID = 1L;

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @ApiModelProperty(notes = "The database generated storage system discovery ID")
    @Column(name = "storage_system_discovery_id")
    @NotNull
    private long storageSystemDiscoveryId;

    @ApiModelProperty(notes = "Storage System ID foreign key")
    @Column(name = "storage_system_id")
    @NotNull
    private long storageSystemId;

    @ApiModelProperty(notes = "Discovery Service start time")
    @Column(name = "start_time")
    private String startTime;

    @ApiModelProperty(notes = "Discovery Service end time")
    @Column(name = "end_time")
    private String endTime;

    @ApiModelProperty(notes = "Total Deletes")
    @Column(name = "total_deletes")
    private long totalDeletes;

    @ApiModelProperty(notes = "Total Upserts")
    @Column(name = "total_upserts")
    private long totalUpserts;

    @ApiModelProperty(notes = "Total inserts")
    @Column(name = "total_inserts")
    private long totalInserts;

    @ApiModelProperty(notes = "Discovery Status")
    @Column(name = "discovery_status")
    private String discoveryStatus;

    @ApiModelProperty(notes = "Error Log")
    @Column(name = "error_log")
    private String errorLog;

    @ApiModelProperty(notes = "Running User")
    @Column(name = "running_user")
    private String runningUser;

    public String getRunningUser() {
        return this.runningUser;
    }

    public void setRunningUser(final String runningUser) {
        this.runningUser = runningUser;
    }

    public String getErrorLog() {
        return this.errorLog;
    }

    public void setErrorLog(final String errorLog) {
        this.errorLog = errorLog;
    }

    public long getStorageSystemDiscoveryId() {
        return this.storageSystemDiscoveryId;
    }

    public void setStorageSystemDiscoveryId(final long storageSystemDiscoveryId) {
        this.storageSystemDiscoveryId = storageSystemDiscoveryId;
    }

    public long getStorageSystemId() {
        return this.storageSystemId;
    }

    public void setStorageSystemId(final long storageSystemId) {
        this.storageSystemId = storageSystemId;
    }

    public String getStartTime() {
        return this.startTime;
    }

    public void setStartTime(final String startTime) {
        this.startTime = startTime;
    }

    public String getEndTime() {
        return this.endTime;
    }

    public void setEndTime(final String endTime) {
        this.endTime = endTime;
    }

    public long getTotalUpserts() {
        return this.totalUpserts;
    }

    public void setTotalUpserts(final long totalUpserts) {
        this.totalUpserts = totalUpserts;
    }

    public long getTotalInserts() {
        return this.totalInserts;
    }

    public void setTotalInserts(final long totalInserts) {
        this.totalInserts = totalInserts;
    }

    public String getDiscoveryStatus() {
        return this.discoveryStatus;
    }

    public void setDiscoveryStatus(final String discoveryStatus) {
        this.discoveryStatus = discoveryStatus;
    }

    public long getTotalDeletes() {
        return this.totalDeletes;
    }

    public void setTotalDeletes(final long totalDeletes) {
        this.totalDeletes = totalDeletes;
    }

    public StorageSystemDiscovery(final long storageSystemId, final String startTime, final String endTime,
            final long totalDeletes, final long totalUpserts, final long totalInserts, final String discoveryStatus,
            final String errorLog, final String runningUser) {
        this.storageSystemId = storageSystemId;
        this.startTime = startTime;
        this.endTime = endTime;
        this.totalDeletes = totalDeletes;
        this.totalUpserts = totalUpserts;
        this.totalInserts = totalInserts;
        this.discoveryStatus = discoveryStatus;
        this.errorLog = errorLog;
        this.runningUser = runningUser;
    }

    public StorageSystemDiscovery() {

    }

}
