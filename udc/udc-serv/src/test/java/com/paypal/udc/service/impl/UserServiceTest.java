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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.Arrays;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.util.ReflectionTestUtils;
import com.paypal.udc.dao.user.UserRepository;
import com.paypal.udc.entity.user.User;
import com.paypal.udc.util.UserUtil;
import com.paypal.udc.util.enumeration.ActiveEnumeration;


@RunWith(SpringRunner.class)
public class UserServiceTest {

    @MockBean
    private UserUtil userUtil;

    @Mock
    private UserRepository userRepository;

    @InjectMocks
    private UserService userService;

    private long userId;
    private String userName, location;
    private User user;
    private List<User> userList;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        this.userId = 0L;
        this.userName = "userName";
        this.location = "location";
        this.user = new User(this.userName, "userFullName", "roles", "managerName", "organization", "qid",
                this.location);
        this.userList = Arrays.asList(this.user);
    }

    @Test
    public void verifyValidGetAllUsers() throws Exception {
        when(this.userRepository.findAll()).thenReturn(this.userList);

        final List<User> result = this.userService.getAllUsers();
        assertEquals(this.userList.size(), result.size());

        verify(this.userRepository).findAll();
    }

    @Test
    public void verifyValidGetUserById() throws Exception {
        when(this.userUtil.validateUser(this.userId)).thenReturn((this.user));
        final User result = this.userService.getUserById(this.userId);
        assertEquals(this.user, result);

        verify(this.userUtil).validateUser(this.userId);
    }

    @Test
    public void verifyValidGetUserByName() throws Exception {
        when(this.userRepository.findByUserName(this.userName)).thenReturn(this.user);

        final User result = this.userService.getUserByName(this.userName);
        assertEquals(this.user, result);

        verify(this.userRepository).findByUserName(this.userName);
    }

    @Test
    public void verifyValidDeleteUser() throws Exception {
        ReflectionTestUtils.setField(this.userService, "isEsWriteEnabled", "true");
        when(this.userUtil.validateUser(this.userId)).thenReturn((this.user));
        when(this.userRepository.save(this.user)).thenReturn(this.user);

        final User result = this.userService.deleteUser(this.userId);
        assertEquals(this.user, result);
        assertEquals(ActiveEnumeration.NO.getFlag(), result.getIsActiveYN());

        verify(this.userRepository).save(this.user);
    }

    @Test
    public void verifyValidEnableUser() throws Exception {
        ReflectionTestUtils.setField(this.userService, "isEsWriteEnabled", "true");
        when(this.userUtil.validateUser(this.userId)).thenReturn((this.user));
        when(this.userRepository.save(this.user)).thenReturn(this.user);

        final User result = this.userService.enableUser(this.userId);
        assertEquals(this.user, result);
        assertEquals(ActiveEnumeration.YES.getFlag(), result.getIsActiveYN());

        verify(this.userRepository).save(this.user);
    }

    @Test
    public void verifyValidAddUser() throws Exception {
        ReflectionTestUtils.setField(this.userService, "isEsWriteEnabled", "true");
        when(this.userRepository.save(this.user)).thenReturn(this.user);

        final User result = this.userService.addUser(this.user);
        assertEquals(this.user, result);
        assertEquals(ActiveEnumeration.YES.getFlag(), result.getIsActiveYN());

        verify(this.userRepository).save(this.user);
    }

}
