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

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
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
import com.paypal.udc.dao.user.UserRepository;
import com.paypal.udc.entity.user.User;
import com.paypal.udc.service.IUserService;


@RunWith(SpringRunner.class)
@WebMvcTest(UserController.class)
public class UserControllerTest {

    private MockMvc mockMvc;

    @MockBean
    private IUserService userService;

    @MockBean
    private UserRepository userRepository;

    @Autowired
    private WebApplicationContext webApplicationContext;

    final Gson gson = new Gson();

    private String userName, userFullName, role, manager, organization, qid, location;
    private Long userId;
    private User user;
    private String jsonUser;
    private List<User> userList;

    class AnyUser implements ArgumentMatcher<User> {
        @Override
        public boolean matches(final User user) {
            return user instanceof User;
        }
    }

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        this.mockMvc = MockMvcBuilders.webAppContextSetup(this.webApplicationContext).build();
        this.userName = "Name";
        this.userFullName = "UserFullName";
        this.role = "Role";
        this.manager = "ManagerName";
        this.organization = "PayPal";
        this.userId = 0L;
        this.qid = "QID";
        this.location = "LOCATION";
        this.user = new User(this.userName, this.userFullName, this.role, this.manager, this.organization, this.qid,
                this.location);
        this.userList = Arrays.asList(this.user);
        this.jsonUser = "{" +
                "\"createdTimestamp\": \"CrTime\", " +
                "\"createdUser\": \"CrUser\", " +
                "\"isActiveYN\": \"Y\", " +
                "\"managerName\": \"ManagerName\", " +
                "\"organization\": \"PayPal\", " +
                "\"roles\": \"Role\", " +
                "\"updatedTimestamp\": \"UpdTime\", " +
                "\"updatedUser\": \"updUser\", " +
                "\"userFullName\": \"UserFullName\", " +
                "\"userId\": 1, " +
                "\"userName\": \"Name\"" +
                "}";
    }

    @Test
    public void verifyValidGetUserByName() throws Exception {
        when(this.userService.getUserByName(this.userName))
                .thenReturn(this.user);

        this.mockMvc.perform(get("/user/userByName/{name:.+}", this.userName)
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.userName").value(this.userName));

        verify(this.userService).getUserByName(this.userName);
    }

    @Test
    public void verifyValidGetUserById() throws Exception {
        when(this.userService.getUserById(this.userId))
                .thenReturn(this.user);

        this.mockMvc.perform(get("/user/user/{id}", this.userId)
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.userName").value(this.userName));

        verify(this.userService).getUserById(this.userId);
    }

    @Test
    public void verifyValidGetAllUsers() throws Exception {
        when(this.userService.getAllUsers())
                .thenReturn(this.userList);

        this.mockMvc.perform(get("/user/users")
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(this.userList.size())));

        verify(this.userService).getAllUsers();
    }

    @Test
    public void verifyValidAddUser() throws Exception {
        when(this.userService.addUser(argThat(new AnyUser())))
                .thenReturn(this.user);

        this.mockMvc.perform(post("/user/user")
                .contentType(MediaType.APPLICATION_JSON)
                .content(this.jsonUser)
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.userName").exists())
                .andExpect(jsonPath("$.userName").value(this.userName));

        verify(this.userService).addUser(argThat(new AnyUser()));
    }

    @Test
    public void verifyValidDeleteUser() throws Exception {
        final String expectedResult = "Deleted " + this.userId;

        when(this.userService.deleteUser(this.userId))
                .thenReturn(this.user);

        this.mockMvc.perform(delete("/user/duser/{id}", this.userId)
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString(expectedResult)));

        verify(this.userService).deleteUser(this.userId);
    }

    @Test
    public void verifyValidActivateUser() throws Exception {
        final String expectedResult = "Enabled " + this.userId;

        when(this.userService.enableUser(this.userId))
                .thenReturn(this.user);

        this.mockMvc.perform(put("/user/euser/{id}", this.userId)
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString(expectedResult)));

        verify(this.userService).enableUser(this.userId);
    }
}
