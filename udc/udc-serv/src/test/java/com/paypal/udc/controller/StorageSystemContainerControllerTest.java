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
import com.paypal.udc.entity.storagesystem.CollectiveStorageSystemContainerObject;
import com.paypal.udc.entity.storagesystem.StorageSystemContainer;
import com.paypal.udc.service.IStorageSystemContainerService;


@RunWith(SpringRunner.class)
@WebMvcTest(StorageSystemContainerController.class)
public class StorageSystemContainerControllerTest {

    private MockMvc mockMvc;

    @MockBean
    private IStorageSystemContainerService storageSystemContainerService;

    @Autowired
    private WebApplicationContext webApplicationContext;

    final Gson gson = new Gson();

    private long storageSystemContainerId, storageSystemId;
    private StorageSystemContainer cluster;
    private String containerName;

    private Long clusterId;
    private CollectiveStorageSystemContainerObject object;
    private List<CollectiveStorageSystemContainerObject> storageSystemContainersWithAttributesList;

    private StorageSystemContainer storageSystemContainer;
    private List<StorageSystemContainer> storageSystemContainers;

    private String jsonStorageSystemContainer;

    class AnyStorageSystemContainer implements ArgumentMatcher<StorageSystemContainer> {
        @Override
        public boolean matches(final StorageSystemContainer storageSystemContainer) {
            return storageSystemContainer instanceof StorageSystemContainer;
        }
    }

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        this.mockMvc = MockMvcBuilders.webAppContextSetup(this.webApplicationContext).build();

        this.storageSystemContainerId = 1L;
        this.storageSystemId = 2L;
        this.containerName = "containerName";
        this.cluster = new StorageSystemContainer(this.storageSystemContainerId, this.storageSystemId,
                this.containerName, "crUser", "crTime", "updUser", "updTime");
        this.clusterId = 3L;
        this.object = new CollectiveStorageSystemContainerObject();
        this.storageSystemContainersWithAttributesList = Arrays.asList(this.object);
        this.storageSystemContainer = new StorageSystemContainer(this.storageSystemContainerId, this.storageSystemId,
                this.containerName, "crUser", "crTime", "updUser", "updTime");
        this.storageSystemContainers = Arrays.asList(this.storageSystemContainer);
        this.jsonStorageSystemContainer = "{" +
                "\"clusterId\": 3, " +
                "\"containerName\": \"containerName\", " +
                "\"createdTimestamp\": \"crTime\", " +
                "\"createdUser\": \"crUser\", " +
                "\"isActiveYN\": \"Y\", " +
                "\"storageSystemContainerId\": 1, " +
                "\"storageSystemId\": 2, " +
                "\"updatedTimestamp\": \"updTime\", " +
                "\"updatedUser\": \"updUser\"" +
                "}";
    }

    @Test
    public void verifyValidGetClusterById() throws Exception {
        when(this.storageSystemContainerService.getStorageSystemContainerById(this.storageSystemContainerId))
                .thenReturn(this.cluster);

        this.mockMvc.perform(get("/storageSystemContainer/storageSystemContainer/{id}", this.storageSystemContainerId)
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.containerName").value(this.containerName));

        verify(this.storageSystemContainerService).getStorageSystemContainerById(this.storageSystemContainerId);
    }

    @Test
    public void verifyValidGetAllStorageSystemContainers() throws Exception {
        when(this.storageSystemContainerService.getAllStorageSystemContainers(this.clusterId))
                .thenReturn(this.storageSystemContainersWithAttributesList);

        this.mockMvc.perform(get("/storageSystemContainer/storageSystemContainers/{clusterId}", this.clusterId)
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(this.storageSystemContainersWithAttributesList.size())));

        verify(this.storageSystemContainerService).getAllStorageSystemContainers(this.clusterId);
    }

    @Test
    public void verifyValidGetAllStorageSystemContainersByStorageSystemId() throws Exception {
        when(this.storageSystemContainerService.getStorageSystemContainersByStorageSystemId(this.storageSystemId))
                .thenReturn(this.storageSystemContainers);

        this.mockMvc
                .perform(
                        get("/storageSystemContainer/storageSystemContainersByStorageSystem/{id}", this.storageSystemId)
                                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(this.storageSystemContainers.size())));

        verify(this.storageSystemContainerService).getStorageSystemContainersByStorageSystemId(this.storageSystemId);
    }

    @Test
    public void verifyValidDeleteStorage() throws Exception {
        final String expectedResult = "Deactivated " + this.storageSystemContainerId;

        when(this.storageSystemContainerService.deleteStorageSystemContainer(this.storageSystemContainerId))
                .thenReturn(this.cluster);

        this.mockMvc
                .perform(delete("/storageSystemContainer/storageSystemContainer/{id}", this.storageSystemContainerId)
                        .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString(expectedResult)));

        verify(this.storageSystemContainerService).deleteStorageSystemContainer(this.storageSystemContainerId);
    }

    @Test
    public void verifyValidAddStorageSystemContainer() throws Exception {
        when(this.storageSystemContainerService.addStorageSystemContainer(argThat(new AnyStorageSystemContainer())))
                .thenReturn(this.storageSystemContainer);

        this.mockMvc.perform(post("/storageSystemContainer/storageSystemContainer")
                .contentType(MediaType.APPLICATION_JSON)
                .content(this.jsonStorageSystemContainer)
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.containerName").exists())
                .andExpect(jsonPath("$.containerName").value(this.containerName));

        verify(this.storageSystemContainerService).addStorageSystemContainer(argThat(new AnyStorageSystemContainer()));
    }

    @Test
    public void verifyValidUpdateStorageSystemContainer() throws Exception {
        when(this.storageSystemContainerService.updateStorageSystemContainer(argThat(new AnyStorageSystemContainer())))
                .thenReturn(this.storageSystemContainer);

        this.mockMvc.perform(put("/storageSystemContainer/storageSystemContainer")
                .contentType(MediaType.APPLICATION_JSON)
                .content(this.jsonStorageSystemContainer)
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.containerName").exists())
                .andExpect(jsonPath("$.containerName").value(this.containerName));

        verify(this.storageSystemContainerService)
                .updateStorageSystemContainer(argThat(new AnyStorageSystemContainer()));
    }
}
