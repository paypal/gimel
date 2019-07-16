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

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;
import io.swagger.annotations.ApiModelProperty;


@Entity
@Table(name = "pc_policy_discovery_metric")
public class PolicyDiscoveryMetric {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @ApiModelProperty(notes = "The ranger discovery metric ID")
    @Column(name = "policy_discovery_metric_id")
    @NotNull
    private long policyDiscoveryMetricId;

    @ApiModelProperty(notes = "Policy Discovery Metric Type")
    @Column(name = "policy_discovery_metric_type")
    private String policyDiscoveryMetricType;

    @ApiModelProperty(notes = "Start Time")
    @Column(name = "start_time")
    private String startTime;

    @ApiModelProperty(notes = "End Time")
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

    @ApiModelProperty(notes = "Running User")
    @Column(name = "running_user")
    private String runningUser;

    public long getPolicyDiscoveryMetricId() {
        return this.policyDiscoveryMetricId;
    }

    public void setPolicyDiscoveryMetricId(final long policyDiscoveryMetricId) {
        this.policyDiscoveryMetricId = policyDiscoveryMetricId;
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

    public long getTotalDeletes() {
        return this.totalDeletes;
    }

    public void setTotalDeletes(final long totalDeletes) {
        this.totalDeletes = totalDeletes;
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

    public String getRunningUser() {
        return this.runningUser;
    }

    public void setRunningUser(final String runningUser) {
        this.runningUser = runningUser;
    }

    public String getPolicyDiscoveryMetricType() {
        return this.policyDiscoveryMetricType;
    }

    public void setPolicyDiscoveryMetricType(final String policyDiscoveryMetricType) {
        this.policyDiscoveryMetricType = policyDiscoveryMetricType;
    }

    public PolicyDiscoveryMetric(final String policyDiscoveryMetricType, final String startTime,
            final String endTime, final long totalDeletes, final long totalUpserts, final long totalInserts,
            final String runningUser) {
        this.policyDiscoveryMetricType = policyDiscoveryMetricType;
        this.startTime = startTime;
        this.endTime = endTime;
        this.totalDeletes = totalDeletes;
        this.totalUpserts = totalUpserts;
        this.totalInserts = totalInserts;
        this.runningUser = runningUser;
    }

    public PolicyDiscoveryMetric() {

    }

}
