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

package com.paypal.udc.entity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;
import io.swagger.annotations.ApiModelProperty;


@Entity
@Table(name = "pc_api_usage_metrics")
public class ApiMetric {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @ApiModelProperty(notes = "The database generated api metric ID")
    @Column(name = "api_usage_metric_id")
    @NotNull
    private long apiUsageMetricId;

    @ApiModelProperty(notes = "Request URL")
    @Column(name = "request_url")
    private String requestUrl;

    @ApiModelProperty(notes = "Unique Request ID")
    @Column(name = "request_id")
    private String requestId;

    @ApiModelProperty(notes = "Is the Storage Category active ?")
    @Column(name = "request_method")
    private String requestMethod;

    @ApiModelProperty(notes = "Response Status of a given request")
    @Column(name = "response_status")
    private long responseStatus;

    @ApiModelProperty(notes = "Request Protocol")
    @Column(name = "request_paradigm")
    private String requestParadigm;

    @ApiModelProperty(notes = "Host Name/ Referrer")
    @Column(name = "host_name")
    private String hostName;

    @ApiModelProperty(notes = "username")
    @Column(name = "user_name")
    private String userName;

    @ApiModelProperty(notes = "Application Name")
    @Column(name = "app_name")
    private String appName;

    @ApiModelProperty(notes = "Additional Params")
    @Column(name = "params")
    private String params;

    @ApiModelProperty(notes = "Request Start time")
    @Column(name = "start_time")
    private long startTime;

    @ApiModelProperty(notes = "Request End time")
    @Column(name = "end_time")
    private long endTime;

    @ApiModelProperty(notes = "total time taken for the request")
    @Column(name = "total_time")
    private float totalTime;

    public long getApiUsageMetricId() {
        return this.apiUsageMetricId;
    }

    public void setApiUsageMetricId(final long apiUsageMetricId) {
        this.apiUsageMetricId = apiUsageMetricId;
    }

    public String getRequestUrl() {
        return this.requestUrl;
    }

    public void setRequestUrl(final String requestUrl) {
        this.requestUrl = requestUrl;
    }

    public String getRequestId() {
        return this.requestId;
    }

    public void setRequestId(final String requestId) {
        this.requestId = requestId;
    }

    public String getRequestMethod() {
        return this.requestMethod;
    }

    public void setRequestMethod(final String requestMethod) {
        this.requestMethod = requestMethod;
    }

    public long getResponseStatus() {
        return this.responseStatus;
    }

    public void setResponseStatus(final long responseStatus) {
        this.responseStatus = responseStatus;
    }

    public String getRequestParadigm() {
        return this.requestParadigm;
    }

    public void setRequestParadigm(final String requestParadigm) {
        this.requestParadigm = requestParadigm;
    }

    public String getHostName() {
        return this.hostName;
    }

    public void setHostName(final String hostName) {
        this.hostName = hostName;
    }

    public String getUserName() {
        return this.userName;
    }

    public void setUserName(final String userName) {
        this.userName = userName;
    }

    public String getAppName() {
        return this.appName;
    }

    public void setAppName(final String appName) {
        this.appName = appName;
    }

    public String getParams() {
        return this.params;
    }

    public void setParams(final String params) {
        this.params = params;
    }

    public long getStartTime() {
        return this.startTime;
    }

    public void setStartTime(final long startTime) {
        this.startTime = startTime;
    }

    public long getEndTime() {
        return this.endTime;
    }

    public void setEndTime(final long endTime) {
        this.endTime = endTime;
    }

    public float getTotalTime() {
        return this.totalTime;
    }

    public void setTotalTime(final float totalTime) {
        this.totalTime = totalTime;
    }

    public ApiMetric(final String requestUrl, final String requestId, final String requestMethod,
            final long responseStatus, final String requestParadigm, final String hostName, final String userName,
            final String appName, final String params, final long startTime, final long endTime,
            final float totalTime) {
        this.requestUrl = requestUrl;
        this.requestId = requestId;
        this.requestMethod = requestMethod;
        this.responseStatus = responseStatus;
        this.requestParadigm = requestParadigm;
        this.hostName = hostName;
        this.userName = userName;
        this.appName = appName;
        this.params = params;
        this.startTime = startTime;
        this.endTime = endTime;
        this.totalTime = totalTime;
    }

    public ApiMetric() {

    }
}
