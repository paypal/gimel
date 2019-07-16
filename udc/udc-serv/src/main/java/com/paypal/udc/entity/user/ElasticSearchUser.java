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

package com.paypal.udc.entity.user;

import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;


@Document(indexName = "#{@userIndexName}", type = "udc")
public class ElasticSearchUser {

    @Id
    private String _id;
    private long userId;
    private String userName;
    private String userFullName;
    private String isActiveYN;
    private String createdUser;
    private String createdTimestamp;
    private String updatedUser;
    private String updatedTimestamp;
    private String roles;
    private String managerName;
    private String organization;
    private String qid;

    public ElasticSearchUser() {

    }

    public ElasticSearchUser(final long userId, final String userName, final String userFullName,
            final String isActiveYN, final String createdUser, final String createdTimestamp, final String updatedUser,
            final String updatedTimestamp, final String roles, final String managerName, final String organization,
            final String qid) {
        this.userId = userId;
        this.userName = userName;
        this.userFullName = userFullName;
        this.isActiveYN = isActiveYN;
        this.createdUser = createdUser;
        this.createdTimestamp = createdTimestamp;
        this.updatedUser = updatedUser;
        this.updatedTimestamp = updatedTimestamp;
        this.roles = roles;
        this.managerName = managerName;
        this.organization = organization;
        this.qid = qid;
    }

    public String get_id() {
        return this._id;
    }

    public void set_id(final String _id) {
        this._id = _id;
    }

    public long getUserId() {
        return this.userId;
    }

    public void setUserId(final long userId) {
        this.userId = userId;
    }

    public String getUserName() {
        return this.userName;
    }

    public void setUserName(final String userName) {
        this.userName = userName;
    }

    public String getUserFullName() {
        return this.userFullName;
    }

    public void setUserFullName(final String userFullName) {
        this.userFullName = userFullName;
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

    public String getRoles() {
        return this.roles;
    }

    public void setRoles(final String roles) {
        this.roles = roles;
    }

    public String getManagerName() {
        return this.managerName;
    }

    public void setManagerName(final String managerName) {
        this.managerName = managerName;
    }

    public String getOrganization() {
        return this.organization;
    }

    public void setOrganization(final String organization) {
        this.organization = organization;
    }

    public String getQid() {
        return this.qid;
    }

    public void setQid(final String qid) {
        this.qid = qid;
    }

}
