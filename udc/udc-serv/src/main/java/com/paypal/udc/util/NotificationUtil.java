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

package com.paypal.udc.util;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import javax.validation.ConstraintViolationException;
import org.apache.commons.lang.math.NumberUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import com.paypal.udc.dao.notification.NotificationRepository;
import com.paypal.udc.entity.notification.Notification;
import com.paypal.udc.exception.ValidationError;
import com.paypal.udc.util.enumeration.ActiveEnumeration;
import com.paypal.udc.util.enumeration.NotificationTypeEnumeration;
import com.paypal.udc.util.enumeration.PriorityEnumeration;


@Component
public class NotificationUtil {
    @Autowired
    private NotificationRepository notificationRepository;

    public Notification addNotification(final Notification notification, final UserUtil userUtil,
            final SimpleDateFormat sdf) throws ValidationError {
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final ValidationError v = new ValidationError();
        try {
            // userUtil.validateUser(notification.getCreatedUser());
            this.validateNotificationType(notification.getNotificationType());
            this.validatePriority(notification.getNotificationPriority());
            this.validateLiveTime(notification.getNotificationLiveTimeInMin());
            notification.setCreatedTimestamp(sdf.format(timestamp));
            notification.setUpdatedTimestamp(sdf.format(timestamp));
            notification.setUpdatedUser(notification.getCreatedUser());
            notification.setIsActiveYN(ActiveEnumeration.YES.getFlag());
            final Notification insertedNotification = this.notificationRepository.save(notification);
            return insertedNotification;
        }
        catch (final ConstraintViolationException e) {
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("Notification content is empty");
            throw v;
        }
    }

    public Notification validateNotifcation(final long notificationId) throws ValidationError {

        final Notification notification = this.notificationRepository.findById(notificationId).orElse(null);
        if (notification == null) {
            final ValidationError v = new ValidationError();
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("Notification ID is not valid");
            throw v;
        }
        return notification;
    }

    public void validatePriority(final String notificationPriority) throws ValidationError {
        final List<String> priorities = Arrays.asList(PriorityEnumeration.values()).stream()
                .map(priority -> priority.getFlag()).collect(Collectors.toList());
        if (!priorities.contains(notificationPriority)) {
            final ValidationError verror = new ValidationError();
            verror.setErrorCode(HttpStatus.BAD_REQUEST);
            verror.setErrorDescription("Invalid Priority");
            throw verror;
        }

    }

    public void validateNotificationType(final String notificationType) throws ValidationError {
        final List<String> types = Arrays.asList(NotificationTypeEnumeration.values()).stream()
                .map(type -> type.getFlag()).collect(Collectors.toList());
        if (!types.contains(notificationType)) {
            final ValidationError verror = new ValidationError();
            verror.setErrorCode(HttpStatus.BAD_REQUEST);
            verror.setErrorDescription("Invalid Notification Type");
            throw verror;
        }

    }

    public void validateLiveTime(final String notificationLiveTimeInMin) throws ValidationError {
        final boolean isNumber = NumberUtils.isNumber(notificationLiveTimeInMin);
        if (!isNumber) {
            final ValidationError verror = new ValidationError();
            verror.setErrorCode(HttpStatus.BAD_REQUEST);
            verror.setErrorDescription("Invalid Live time in minutes");
            throw verror;
        }
    }
}
