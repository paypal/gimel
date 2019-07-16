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

import java.io.Serializable;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.validation.constraints.Size;
import io.swagger.annotations.ApiModelProperty;


@Entity
@Table(name = "pc_users")
public class User implements Serializable {

    private static final long serialVersionUID = 1L;

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @ApiModelProperty(notes = "The database generated user ID")
    @Column(name = "user_id")
    private long userId;

    @ApiModelProperty(notes = "Name of the user")
    @Column(name = "user_name")
    @Size(min = 1, message = "Name need to have atleast 1 character")
    private String userName;

    @ApiModelProperty(notes = "Full Name of the user")
    @Column(name = "user_full_name")
    private String userFullName;

    @ApiModelProperty(notes = "Is the User active ?")
    @Column(name = "is_active_y_n")
    private String isActiveYN;

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

    @ApiModelProperty(notes = "Roles")
    @Column(name = "roles")
    private String roles;

    @ApiModelProperty(notes = "Manager Name")
    @Column(name = "manager_name")
    private String managerName;

    @ApiModelProperty(notes = "Organization")
    @Column(name = "organization")
    private String organization;

    @ApiModelProperty(notes = "QID")
    @Column(name = "qid")
    private String qid;

    @ApiModelProperty(notes = "Location")
    @Column(name = "location")
    private String location;

    public String getQid() {
        return this.qid;
    }

    public void setQid(final String qid) {
        this.qid = qid;
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

    public User() {

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

    public String getLocation() {
        return this.location;
    }

    public void setLocation(final String location) {
        this.location = location;
    }

    public User(final String userName, final String userFullName, final String roles, final String managerName,
            final String organization, final String qid, final String location) {
        this.userName = userName;
        this.userFullName = userFullName;
        this.roles = roles;
        this.managerName = managerName;
        this.organization = organization;
        this.qid = qid;
        this.location = location;
    }

}
