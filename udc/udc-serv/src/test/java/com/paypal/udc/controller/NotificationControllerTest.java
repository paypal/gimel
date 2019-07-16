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

import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import java.util.Arrays;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;
import com.google.gson.Gson;
import com.paypal.udc.dao.notification.NotificationRepository;
import com.paypal.udc.entity.notification.Notification;
import com.paypal.udc.service.INotificationService;


@RunWith(SpringRunner.class)
@WebMvcTest(NotificationController.class)
public class NotificationControllerTest {

    private MockMvc mockMvc;

    @MockBean
    private INotificationService notificationService;

    @MockBean
    private NotificationRepository notificationRepository;

    @Autowired
    private WebApplicationContext webApplicationContext;

    final Gson gson = new Gson();

    private String notificationType, notificationPriority, notificationContent, notificationMessagePage,
            notificationLiveTime;
    private Long notificationId;
    private Notification notification;
    private String jsonEntity;
    private List<Notification> notifications;

    class AnyNotification implements ArgumentMatcher<Notification> {
        @Override
        public boolean matches(final Notification notification) {
            return notification instanceof Notification;
        }
    }

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        this.mockMvc = MockMvcBuilders.webAppContextSetup(this.webApplicationContext).build();
        this.notificationType = "NEW FEATURE";
        this.notificationPriority = "P0";
        this.notificationContent = "Release Unit testing";
        this.notificationMessagePage = "DATASET";
        this.notificationLiveTime = "1440";
        this.notificationId = 0L;

        this.notification = new Notification(this.notificationType, this.notificationPriority, this.notificationContent,
                this.notificationMessagePage, "", this.notificationLiveTime, "createdUser", "", "createdUser",
                "", "Y");
        this.notifications = Arrays.asList(this.notification);
        this.jsonEntity = "{" +
                "\"createdTimestamp\": \"CrTime\", " +
                "\"createdUser\": \"CrUser\", " +
                "\"isActiveYN\": \"Y\", " +
                "\"updatedTimestamp\": \"UpdTime\", " +
                "\"updatedUser\": \"updUser\", " +
                "\"entityDescription\": \"entityDescription\", " +
                "\"notificationId\": 1, " +
                "\"notificationContent\": \"Release Unit testing\"" +
                "}";
    }

    @Test
    public void verifyValidGetNotificationById() throws Exception {
        when(this.notificationService.getNotificationById(this.notificationId))
                .thenReturn(this.notification);

        this.mockMvc.perform(get("/notification/notification/{id}", this.notificationId)
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.notificationId").value(this.notificationId));

        verify(this.notificationService).getNotificationById(this.notificationId);
    }

    @Test
    public void verifyValidGetAllNotifications() throws Exception {
        when(this.notificationService.getAllNotifications())
                .thenReturn(this.notifications);

        this.mockMvc.perform(get("/notification/notifications")
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(this.notifications.size())));

        verify(this.notificationService).getAllNotifications();
    }

    @Test
    public void verifyValidAddNotification() throws Exception {
        when(this.notificationService.addNotification(argThat(new AnyNotification())))
                .thenReturn(this.notification);

        this.mockMvc.perform(post("/notification/notification")
                .contentType(MediaType.APPLICATION_JSON)
                .content(this.jsonEntity)
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.notificationContent").exists())
                .andExpect(jsonPath("$.notificationContent").value(this.notificationContent));

        verify(this.notificationService).addNotification(argThat(new AnyNotification()));
    }

    @Test
    public void verifyValidUpdateNotification() throws Exception {
        when(this.notificationService.updateNotification(argThat(new AnyNotification())))
                .thenReturn(this.notification);

        this.mockMvc.perform(put("/notification/notification")
                .contentType(MediaType.APPLICATION_JSON)
                .content(this.jsonEntity)
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.notificationId").exists())
                .andExpect(jsonPath("$.notificationId").value(this.notificationId));

        verify(this.notificationService).updateNotification(argThat(new AnyNotification()));
    }

}
