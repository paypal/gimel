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

package com.paypal.udc.controller;

import java.text.ParseException;
import java.util.List;
import javax.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import com.google.gson.Gson;
import com.paypal.udc.entity.notification.Notification;
import com.paypal.udc.exception.ValidationError;
import com.paypal.udc.service.INotificationService;
import com.paypal.udc.util.enumeration.UserAttributeEnumeration;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;


@RestController
@RequestMapping("notification")
@Api(value = "NotificationService", description = "Operations pertaining to Notifications")
public class NotificationController {

    final static Logger logger = LoggerFactory.getLogger(NotificationController.class);
    final Gson gson = new Gson();
    private INotificationService notificationService;
    private HttpServletRequest request;
    private String userType;

    @Autowired
    private NotificationController(final INotificationService notificationService,
            final HttpServletRequest request) {

        this.notificationService = notificationService;
        this.request = request;
        this.userType = UserAttributeEnumeration.SUCCESS.getFlag();

    }

    @ApiOperation(value = "View the notification based on ID", response = Notification.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved Notification"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("notification/{id}")
	public ResponseEntity<?> getNotificationById(@PathVariable("id") final Long id) {
		final Notification notification;
		try {
			notification = this.notificationService.getNotificationById(id);
			return new ResponseEntity<Notification>(notification, HttpStatus.OK);
		} catch (final ValidationError e) {
			return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
		}
	}

    @ApiOperation(value = "View a list of available Notifications", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved list"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("notifications")
    public ResponseEntity<?> getAllNotifications() {
		List<Notification> list;
		try {
			list = this.notificationService.getAllNotifications();
			return new ResponseEntity<List<Notification>>(list, HttpStatus.OK);
		} catch (final ParseException e) {
			return new ResponseEntity<String>(this.gson.toJson(e.getMessage()), HttpStatus.BAD_REQUEST);
		}
    }

    @ApiOperation(value = "Bulk upload for notifications", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully inserted notifications"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PostMapping("notifications")
    public ResponseEntity<?> addNotifications(@RequestBody final List<Notification> notifications) {
        try {
            final List<Notification> insertedNotifications = this.notificationService.addNotifications(notifications);
            return new ResponseEntity<List<Notification>>(insertedNotifications, HttpStatus.CREATED);
        }
        catch (final ValidationError e) {
            return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
        }
    }

    @ApiOperation(value = "Insert a notification", response = String.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully inserted notification"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PostMapping("notification")
    public ResponseEntity<String> addNotification(@RequestBody final Notification notification) {
        Notification insertedNotification;
        try {
            insertedNotification = this.notificationService.addNotification(notification);
            return new ResponseEntity<String>(this.gson.toJson(insertedNotification), HttpStatus.CREATED);
        }
        catch (final ValidationError e) {
            return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
        }
    }

    @ApiOperation(value = "Update a notification based on Input", response = String.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully updated Notification"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PutMapping("notification")
    public ResponseEntity<String> updateNotification(@RequestBody final Notification notification) {
        Notification updatedNotification;
        try {
            updatedNotification = this.notificationService.updateNotification(notification);
            return new ResponseEntity<String>(this.gson.toJson(updatedNotification), HttpStatus.OK);
        }
        catch (final ValidationError e) {
            return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
        }
    }

}
