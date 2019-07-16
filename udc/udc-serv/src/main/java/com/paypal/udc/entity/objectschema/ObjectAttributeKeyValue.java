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

package com.paypal.udc.entity.objectschema;

import java.io.Serializable;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import io.swagger.annotations.ApiModelProperty;


@Entity
@Table(name = "pc_storage_object_attribute_custom_key_value")
public class ObjectAttributeKeyValue implements Serializable {

    private static final long serialVersionUID = 1L;

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @ApiModelProperty(notes = "The Auto generated object attribute key value id")
    @Column(name = "object_attribute_key_value_id")
    private long objectAttributeKeyValueId;

    @ApiModelProperty(notes = "Object ID foreign key")
    @Column(name = "object_id")
    @NotNull
    private long objectId;

    @ApiModelProperty(notes = "Object Attribute key")
    @Column(name = "object_attribute_key")
    @Size(min = 1, message = "key need to have atleast 1 character")
    private String objectAttributeKey;

    @ApiModelProperty(notes = "Object Attribute Value")
    @Column(name = "object_attribute_value")
    private String objectAttributeValue;

    @ApiModelProperty(notes = "is active flag")
    @Column(name = "is_active_y_n")
    @NotNull
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

    public String getIsActiveYN() {
        return this.isActiveYN;
    }

    public void setIsActiveYN(final String isActiveYN) {
        this.isActiveYN = isActiveYN;
    }

    public long getObjectId() {
        return this.objectId;
    }

    public void setObjectId(final long objectId) {
        this.objectId = objectId;
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

    public ObjectAttributeKeyValue() {

    }

    public long getObjectAttributeKeyValueId() {
        return this.objectAttributeKeyValueId;
    }

    public void setObjectAttributeKeyValueId(final long objectAttributeKeyValueId) {
        this.objectAttributeKeyValueId = objectAttributeKeyValueId;
    }

    public String getObjectAttributeKey() {
        return this.objectAttributeKey;
    }

    public void setObjectAttributeKey(final String objectAttributeKey) {
        this.objectAttributeKey = objectAttributeKey;
    }

    public String getObjectAttributeValue() {
        return this.objectAttributeValue;
    }

    public void setObjectAttributeValue(final String objectAttributeValue) {
        this.objectAttributeValue = objectAttributeValue;
    }

    public ObjectAttributeKeyValue(final long objectId, final String objectAttributeKey,
            final String objectAttributeValue, final long storageSystemId, final String isActiveYN,
            final String createdUser, final String createdTimestamp, final String updatedUser,
            final String updatedTimestamp) {
        this.objectId = objectId;
        this.objectAttributeKey = objectAttributeKey;
        this.objectAttributeValue = objectAttributeValue;
        this.isActiveYN = isActiveYN;
        this.createdUser = createdUser;
        this.createdTimestamp = createdTimestamp;
        this.updatedUser = updatedUser;
        this.updatedTimestamp = updatedTimestamp;
    }

}
