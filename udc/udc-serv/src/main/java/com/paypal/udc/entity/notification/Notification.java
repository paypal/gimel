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

package com.paypal.udc.entity.notification;

import java.io.Serializable;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Transient;
import javax.validation.constraints.Size;
import io.swagger.annotations.ApiModelProperty;


@Entity
@Table(name = "pc_notifications")
public class Notification implements Serializable {

    private static final long serialVersionUID = 1L;

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @ApiModelProperty(notes = "The database generated notification ID")
    @Column(name = "notification_id")
    private long notificationId;

    @ApiModelProperty(notes = "Notification Type")
    @Column(name = "notification_type")
    private String notificationType;

    @ApiModelProperty(notes = "Notification Priority")
    @Column(name = "notification_priority")
    private String notificationPriority;

    @ApiModelProperty(notes = "Notification Content")
    @Column(name = "notification_content")
    @Size(min = 1, message = "Name need to have atleast 1 character")
    private String notificationContent;

    @ApiModelProperty(notes = "Notification Message Page")
    @Column(name = "notification_message_page")
    private String notificationMessagePage;

    @ApiModelProperty(notes = "Notification Expiry in Minutes")
    @Column(name = "notification_live_time")
    private String notificationLiveTimeInMin;

    @ApiModelProperty(notes = "Reference Url")
    @Column(name = "reference_url")
    private String referenceUrl;

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

    @ApiModelProperty(notes = "Is Active Y N")
    @Column(name = "is_active_y_n")
    private String isActiveYN;

    @Transient
    private boolean activeYN;

    public Notification() {

    }

    public Notification(final long notificationId, final String notificationType, final String notificationPriority,
            final String notificationContent, final String notificationMessagePage, final String referenceUrl,
            final String notificationLiveTimeInMin, final String createdUser, final String createdTimestamp,
            final String updatedUser, final String updatedTimestamp, final String isActiveYN) {
        this.notificationId = notificationId;
        this.notificationType = notificationType;
        this.notificationPriority = notificationPriority;
        this.notificationContent = notificationContent;
        this.notificationMessagePage = notificationMessagePage;
        this.referenceUrl = referenceUrl;
        this.notificationLiveTimeInMin = notificationLiveTimeInMin;
        this.createdUser = createdUser;
        this.createdTimestamp = createdTimestamp;
        this.updatedTimestamp = updatedTimestamp;
        this.updatedUser = updatedUser;
        this.isActiveYN = isActiveYN;
    }

    public Notification(final String notificationType, final String notificationPriority,
            final String notificationContent, final String notificationMessagePage, final String referenceUrl,
            final String notificationLiveTimeInMin, final String createdUser, final String createdTimestamp,
            final String updatedUser, final String updatedTimestamp, final String isActiveYN) {
        this.notificationType = notificationType;
        this.notificationPriority = notificationPriority;
        this.notificationContent = notificationContent;
        this.notificationMessagePage = notificationMessagePage;
        this.referenceUrl = referenceUrl;
        this.notificationLiveTimeInMin = notificationLiveTimeInMin;
        this.createdUser = createdUser;
        this.createdTimestamp = createdTimestamp;
        this.updatedTimestamp = updatedTimestamp;
        this.updatedUser = updatedUser;
        this.isActiveYN = isActiveYN;
    }

    public String getIsActiveYN() {
        return this.isActiveYN;
    }

    public void setIsActiveYN(final String isActiveYN) {
        this.isActiveYN = isActiveYN;
    }

    public boolean isActiveYN() {
        return this.activeYN;
    }

    public void setActiveYN(final boolean activeYN) {
        this.activeYN = activeYN;
    }

    public String getReferenceUrl() {
        return this.referenceUrl;
    }

    public void setReferenceUrl(final String referenceUrl) {
        this.referenceUrl = referenceUrl;
    }

    public String getNotificationMessagePage() {
        return this.notificationMessagePage;
    }

    public void setNotificationMessagePage(final String notificationMessagePage) {
        this.notificationMessagePage = notificationMessagePage;
    }

    public String getNotificationLiveTimeInMin() {
        return this.notificationLiveTimeInMin;
    }

    public void setNotificationLiveTimeInMin(final String notificationLiveTimeInMin) {
        this.notificationLiveTimeInMin = notificationLiveTimeInMin;
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

    public long getNotificationId() {
        return this.notificationId;
    }

    public void setNotificationId(final long notificationId) {
        this.notificationId = notificationId;
    }

    public String getNotificationType() {
        return this.notificationType;
    }

    public void setNotificationType(final String notificationType) {
        this.notificationType = notificationType;
    }

    public String getNotificationPriority() {
        return this.notificationPriority;
    }

    public void setNotificationPriority(final String notificationPriority) {
        this.notificationPriority = notificationPriority;
    }

    public String getNotificationContent() {
        return this.notificationContent;
    }

    public void setNotificationContent(final String notificationContent) {
        this.notificationContent = notificationContent;
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

    @Override
    public String toString() {
        return "Notification [notificationId=" + this.notificationId + ", notificationType=" + this.notificationType
                + ", notificationPriority=" + this.notificationPriority + ", notificationContent="
                + this.notificationContent + ", notificationMessagePage=" + this.notificationMessagePage
                + ", notificationLiveTimeInMin=" + this.notificationLiveTimeInMin + ", referenceUrl="
                + this.referenceUrl + ", createdUser=" + this.createdUser + ", createdTimestamp="
                + this.createdTimestamp + ", updatedUser=" + this.updatedUser + ", updatedTimestamp="
                + this.updatedTimestamp + "]";
    }

}
