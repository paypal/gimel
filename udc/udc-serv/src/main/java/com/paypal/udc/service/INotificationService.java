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

package com.paypal.udc.service;

import java.text.ParseException;
import java.util.List;
import com.paypal.udc.entity.notification.Notification;
import com.paypal.udc.exception.ValidationError;


public interface INotificationService {

    List<Notification> getAllNotifications() throws ParseException;

    Notification getNotificationById(long notificationId) throws ValidationError;

    Notification addNotification(Notification user) throws ValidationError;

    Notification updateNotification(Notification user) throws ValidationError;

    List<Notification> addNotifications(final List<Notification> notifications) throws ValidationError;

}
