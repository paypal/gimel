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

package com.paypal.udc.service.impl;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.TransactionSystemException;
import com.paypal.udc.dao.notification.NotificationRepository;
import com.paypal.udc.entity.notification.Notification;
import com.paypal.udc.exception.ValidationError;
import com.paypal.udc.service.INotificationService;
import com.paypal.udc.util.NotificationUtil;
import com.paypal.udc.util.UserUtil;


@Service
public class NotificationService implements INotificationService {

    @Autowired
    private NotificationRepository notificationRepository;
    @Autowired
    private NotificationUtil notificationUtil;
    @Autowired
    private UserUtil userUtil;

    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    final static Logger logger = LoggerFactory.getLogger(NotificationService.class);

    @Override
    public Notification getNotificationById(final long notificationId) throws ValidationError {
        return this.notificationUtil.validateNotifcation(notificationId);
    }

    @Override
    public List<Notification> getAllNotifications() throws ParseException {

        final Iterator<Notification> myIterator = this.notificationRepository.findAll().iterator();
        final List<Notification> notifications = IteratorUtils.toList(myIterator);
        final String currentDate = sdf.format(new Date());
        final Date date = sdf.parse(currentDate);
        for (final Notification notification : notifications) {
            final Date tempDate = sdf.parse(notification.getCreatedTimestamp());
            final int tempNotificationLiveTime = Integer.parseInt(notification.getNotificationLiveTimeInMin());
            final long diff = date.getTime() - tempDate.getTime();
            final long diffMinutes = TimeUnit.MINUTES.convert(diff, TimeUnit.MILLISECONDS);
            if (diffMinutes < tempNotificationLiveTime) {
                notification.setActiveYN(true);
            }
            else {
                notification.setActiveYN(false);
            }
        }

        return notifications;
    }

    @Override
    public List<Notification> addNotifications(final List<Notification> notifications)
            throws ValidationError {
        final List<Notification> outputs = new ArrayList<Notification>();
        for (final Notification notification : notifications) {
            outputs.add(this.notificationUtil.addNotification(notification, this.userUtil, sdf));
        }
        return outputs;
    }

    @Override
    public Notification addNotification(final Notification notification) throws ValidationError {
        return this.notificationUtil.addNotification(notification, this.userUtil, sdf);
    }

    @Override
    public Notification updateNotification(final Notification notification) throws ValidationError {
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final ValidationError v = new ValidationError();
        Notification retreivedNotification = this.notificationUtil
                .validateNotifcation(notification.getNotificationId());
        try {
            // this.userUtil.validateUser(notification.getUpdatedUser());
            retreivedNotification.setNotificationType(notification.getNotificationType() == null
                    ? retreivedNotification.getNotificationType() : notification.getNotificationType());
            retreivedNotification.setNotificationContent(notification.getNotificationContent() == null
                    ? retreivedNotification.getNotificationContent() : notification.getNotificationContent());
            retreivedNotification.setNotificationPriority(notification.getNotificationPriority() == null
                    ? retreivedNotification.getNotificationPriority() : notification.getNotificationPriority());
            retreivedNotification.setNotificationMessagePage(notification.getNotificationMessagePage() == null
                    ? retreivedNotification.getNotificationMessagePage() : notification.getNotificationMessagePage());
            retreivedNotification.setNotificationLiveTimeInMin(notification.getNotificationLiveTimeInMin() == null
                    ? retreivedNotification.getNotificationLiveTimeInMin()
                    : notification.getNotificationLiveTimeInMin());
            retreivedNotification.setReferenceUrl(notification.getReferenceUrl() == null
                    ? retreivedNotification.getReferenceUrl() : notification.getReferenceUrl());
            retreivedNotification.setUpdatedUser(notification.getUpdatedUser());
            retreivedNotification.setUpdatedTimestamp(sdf.format(timestamp));
            retreivedNotification = this.notificationRepository.save(retreivedNotification);
            return retreivedNotification;
        }
        catch (final TransactionSystemException e) {
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("Notification content is empty");
            throw v;
        }

    }

}
