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
import com.paypal.udc.dao.ownership.DatasetOwnershipMapRepository;
import com.paypal.udc.entity.ownership.DatasetOwnershipMap;
import com.paypal.udc.service.IOwnershipDatasetService;


@RunWith(SpringRunner.class)
@WebMvcTest(DatasetOwnershipController.class)
public class DatasetOwnershipControllerTest {

    private MockMvc mockMvc;

    @MockBean
    private IOwnershipDatasetService ownershipDatasetService;

    @MockBean
    private DatasetOwnershipMapRepository domr;

    @Autowired
    private WebApplicationContext webApplicationContext;

    final Gson gson = new Gson();

    private String ownerName, containerName, objectName, ownershipComment, ownerEmail,
            emailIlist, systemName;
    private Long datasetOwnershipMapId, storageDatasetId, providerId, storageSystemId;
    private DatasetOwnershipMap datasetOwnershipMap;
    private String jsonEntity;
    private List<DatasetOwnershipMap> owners;

    class AnyDatasetOwnershipMap implements ArgumentMatcher<DatasetOwnershipMap> {
        @Override
        public boolean matches(final DatasetOwnershipMap datasetOwnershipMap) {
            return datasetOwnershipMap instanceof DatasetOwnershipMap;
        }
    }

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        this.mockMvc = MockMvcBuilders.webAppContextSetup(this.webApplicationContext).build();
        this.ownerName = "owner_name";
        this.containerName = "container_name";
        this.objectName = "object_name";
        this.ownershipComment = "comment";
        this.ownerEmail = "help-udc";
        this.emailIlist = "help-udc";

        this.datasetOwnershipMapId = 1L;
        this.storageDatasetId = 1L;
        this.providerId = 1L;
        this.storageSystemId = 1L;
        this.systemName = "system_name";

        this.datasetOwnershipMap = new DatasetOwnershipMap(this.storageDatasetId, this.providerId, this.storageSystemId,
                this.ownerName, this.containerName, this.objectName, this.ownershipComment, this.ownerEmail,
                this.emailIlist, "CrUser", "CrTime", "updUser", "UpdTime", "MiscOwners", "N");
        this.datasetOwnershipMap.setDatasetOwnershipMapId(this.datasetOwnershipMapId);
        this.owners = Arrays.asList(this.datasetOwnershipMap);
        this.jsonEntity = "{" +
                "\"createdTimestamp\": \"CrTime\", " +
                "\"createdUser\": \"CrUser\", " +
                "\"isActiveYN\": \"Y\", " +
                "\"updatedTimestamp\": \"UpdTime\", " +
                "\"updatedUser\": \"updUser\", " +
                "\"ownerName\": \"owner_name\", " +
                "\"datasetOwnershipMapId\": 1, " +
                "\"ownershipComment\": \"comment\"" +
                "}";
    }

    @Test
    public void verifyValidGetOwnersById() throws Exception {
        when(this.ownershipDatasetService.getOwnersByDatasetId(this.storageDatasetId))
                .thenReturn(this.owners);

        this.mockMvc.perform(get("/ownership/owners/{id}", this.storageDatasetId)
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(this.owners.size())));

        verify(this.ownershipDatasetService).getOwnersByDatasetId(this.storageDatasetId);
    }

    @Test
    public void verifyValidGetOwnersBySystemContainerObjectAndOwner() throws Exception {
        when(this.ownershipDatasetService.getOwnerBySystemContainerObjectAndOwnerName(this.systemName,
                this.containerName, this.objectName, this.ownerName))
                        .thenReturn(this.datasetOwnershipMap);

        this.mockMvc.perform(get("/ownership/owner/{systemName}/{containerName:.+}/{objectName:.+}/{ownerName:.+}",
                this.systemName, this.containerName, this.objectName, this.ownerName)
                        .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.datasetOwnershipMapId").value(this.datasetOwnershipMapId));

        verify(this.ownershipDatasetService).getOwnerBySystemContainerObjectAndOwnerName(this.systemName,
                this.containerName, this.objectName, this.ownerName);
    }

    @Test
    public void verifyValidGetOwnersBySystemContainerObject() throws Exception {
        when(this.ownershipDatasetService.getOwnersBySystemContainerAndObject(this.systemName,
                this.containerName, this.objectName))
                        .thenReturn(this.owners);

        this.mockMvc.perform(get("/ownership/owner/{systemName}/{containerName:.+}/{objectName:.+}",
                this.systemName, this.containerName, this.objectName)
                        .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(this.owners.size())));

        verify(this.ownershipDatasetService).getOwnersBySystemContainerAndObject(this.systemName,
                this.containerName, this.objectName);
    }

    @Test
    public void verifyValidGetAllOwners() throws Exception {
        when(this.ownershipDatasetService.getAllOwners())
                .thenReturn(this.owners);

        this.mockMvc.perform(get("/ownership/owners")
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(this.owners.size())));

        verify(this.ownershipDatasetService).getAllOwners();
    }

    @Test
    public void verifyValidGetAllNewOwners() throws Exception {
        when(this.ownershipDatasetService.getAllNewOwners())
                .thenReturn(this.owners);

        this.mockMvc.perform(get("/ownership/newOwners")
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(this.owners.size())));

        verify(this.ownershipDatasetService).getAllNewOwners();
    }

    @Test
    public void verifyValidAddOwnership() throws Exception {
        when(this.ownershipDatasetService.addDatasetOwnershipMap(argThat(new AnyDatasetOwnershipMap())))
                .thenReturn(this.owners);

        this.mockMvc.perform(post("/ownership/owner")
                .contentType(MediaType.APPLICATION_JSON)
                .content(this.jsonEntity)
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(this.owners.size())));

        verify(this.ownershipDatasetService).addDatasetOwnershipMap(argThat(new AnyDatasetOwnershipMap()));
    }

    @Test
    public void verifyValidUpdateOwnership() throws Exception {
        when(this.ownershipDatasetService.updateDatasetOwnershipMap(argThat(new AnyDatasetOwnershipMap())))
                .thenReturn(this.datasetOwnershipMap);

        this.mockMvc.perform(put("/ownership/owner")
                .contentType(MediaType.APPLICATION_JSON)
                .content(this.jsonEntity)
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.ownerName").exists())
                .andExpect(jsonPath("$.ownerName").value(this.ownerName));

        verify(this.ownershipDatasetService).updateDatasetOwnershipMap(argThat(new AnyDatasetOwnershipMap()));
    }

}
