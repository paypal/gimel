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
import com.paypal.udc.dao.entity.EntityRepository;
import com.paypal.udc.entity.entity.Entity;
import com.paypal.udc.service.IEntityService;


@RunWith(SpringRunner.class)
@WebMvcTest(EntityController.class)
public class EntityControllerTest {

    private MockMvc mockMvc;

    @MockBean
    private IEntityService entityService;

    @MockBean
    private EntityRepository entityRepository;

    @Autowired
    private WebApplicationContext webApplicationContext;

    final Gson gson = new Gson();

    private String entityName, entityDescription;
    private Long entityId;
    private Entity entity;
    private String jsonEntity;
    private List<Entity> entityList;

    class AnyEntity implements ArgumentMatcher<Entity> {
        @Override
        public boolean matches(final Entity entity) {
            return entity instanceof Entity;
        }
    }

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        this.mockMvc = MockMvcBuilders.webAppContextSetup(this.webApplicationContext).build();
        this.entityName = "Entity1";
        this.entityDescription = "entityDescription";
        this.entityId = 0L;
        this.entity = new Entity(this.entityName, this.entityDescription, "Y", "CrUser",
                "CrTime", "UpdUser", "UpdTime");
        this.entityList = Arrays.asList(this.entity);
        this.jsonEntity = "{" +
                "\"createdTimestamp\": \"CrTime\", " +
                "\"createdUser\": \"CrUser\", " +
                "\"isActiveYN\": \"Y\", " +
                "\"updatedTimestamp\": \"UpdTime\", " +
                "\"updatedUser\": \"updUser\", " +
                "\"entityDescription\": \"entityDescription\", " +
                "\"entityId\": 1, " +
                "\"entityName\": \"Entity1\"" +
                "}";
    }

    @Test
    public void verifyValidGetEntityById() throws Exception {
        when(this.entityService.getEntityById(this.entityId))
                .thenReturn(this.entity);

        this.mockMvc.perform(get("/entity/entity/{id}", this.entityId)
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.entityId").value(this.entityId));

        verify(this.entityService).getEntityById(this.entityId);
    }

    @Test
    public void verifyValidGetEntityByName() throws Exception {
        when(this.entityService.getEntityByName(this.entityName))
                .thenReturn(this.entity);

        this.mockMvc.perform(get("/entity/entityByName/{name:.+}", this.entityName)
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.entityName").value(this.entityName));

        verify(this.entityService).getEntityByName(this.entityName);
    }

    @Test
    public void verifyValidGetAllEntities() throws Exception {
        when(this.entityService.getAllEntities())
                .thenReturn(this.entityList);

        this.mockMvc.perform(get("/entity/entities")
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(this.entityList.size())));

        verify(this.entityService).getAllEntities();
    }

    @Test
    public void verifyValidAddEntity() throws Exception {
        when(this.entityService.addEntity(argThat(new AnyEntity())))
                .thenReturn(this.entity);

        this.mockMvc.perform(post("/entity/entity")
                .contentType(MediaType.APPLICATION_JSON)
                .content(this.jsonEntity)
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.entityName").exists())
                .andExpect(jsonPath("$.entityName").value(this.entityName));

        verify(this.entityService).addEntity(argThat(new AnyEntity()));
    }

    @Test
    public void verifyValidDeActivateEntity() throws Exception {
        final String expectedResult = "Deactivated " + this.entityId;

        when(this.entityService.deActivateEntity(this.entityId))
                .thenReturn(this.entity);

        this.mockMvc.perform(delete("/entity/dentity/{id}", this.entityId)
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString(expectedResult)));

        verify(this.entityService).deActivateEntity(this.entityId);
    }

    @Test
    public void verifyValidUpdateEntity() throws Exception {
        when(this.entityService.updateEntity(argThat(new AnyEntity())))
                .thenReturn(this.entity);

        this.mockMvc.perform(put("/entity/entity")
                .contentType(MediaType.APPLICATION_JSON)
                .content(this.jsonEntity)
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.entityId").exists())
                .andExpect(jsonPath("$.entityId").value(this.entityId));

        verify(this.entityService).updateEntity(argThat(new AnyEntity()));
    }

    @Test
    public void verifyValidReActivateEntity() throws Exception {
        final String expectedResult = "Reactivated " + this.entityId;

        when(this.entityService.reActivateEntity(this.entityId))
                .thenReturn(this.entity);

        this.mockMvc.perform(put("/entity/eentity/{id}", this.entityId)
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString(expectedResult)));

        verify(this.entityService).reActivateEntity(this.entityId);
    }
}
