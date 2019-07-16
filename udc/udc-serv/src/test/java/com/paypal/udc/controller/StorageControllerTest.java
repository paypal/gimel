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
import com.paypal.udc.dao.storagecategory.StorageRepository;
import com.paypal.udc.entity.storagecategory.Storage;
import com.paypal.udc.service.IStorageService;


@RunWith(SpringRunner.class)
@WebMvcTest(StorageController.class)
public class StorageControllerTest {

    private MockMvc mockMvc;

    @MockBean
    private IStorageService storageService;

    @MockBean
    private StorageRepository storageRepository;

    @Autowired
    private WebApplicationContext webApplicationContext;


    final Gson gson = new Gson();

    private Long storageId;
    private String storageName, storageDescription, createdUser, createdTimestamp, updatedUser, updatedTimestamp;
    private Storage storage;
    private String jsonStorage;
    private List<Storage> storageList;

    class AnyStorage implements ArgumentMatcher<Storage> {
        @Override
        public boolean matches(final Storage storage) {
            return storage instanceof Storage;
        }
    }

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        this.mockMvc = MockMvcBuilders.webAppContextSetup(this.webApplicationContext).build();

        this.storageId = 1L;
        this.storageName = "StorageName";
        this.storageDescription = "Storage Description";
        this.createdUser = "crUser";
        this.createdTimestamp = "crTime";
        this.updatedUser = "updUser";
        this.updatedTimestamp = "updTime";
        this.storage = new Storage(this.storageId, this.storageName, this.storageDescription, this.createdUser,
                this.createdTimestamp, this.updatedUser, this.updatedTimestamp);
        this.storageList = Arrays.asList(this.storage);
        this.jsonStorage = "{" +
                "\"createdTimestamp\": \"string\", " +
                "\"createdUser\": \"string\", " +
                "\"isActiveYN\": \"string\", " +
                "\"storageDescription\": \"string\", " +
                "\"storageId\": 1, " +
                "\"storageName\": \"string\", " +
                "\"updatedUser\": \"string\"" +
                "}";
    }

    @Test
    public void verifyValidGetStorageById() throws Exception {
        when(this.storageService.getStorageById(this.storageId))
                .thenReturn(this.storage);

        this.mockMvc.perform(get("/storage/storage/{id}", this.storageId)
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.storageName").value(this.storageName));

        verify(this.storageService).getStorageById(this.storageId);
    }

    @Test
    public void verifyValidGetStorageByTitle() throws Exception {
        when(this.storageService.getStorageByName(this.storageName))
                .thenReturn(this.storage);

        this.mockMvc.perform(get("/storage/storageByName/{name:.+}", this.storageName)
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.storageName").value(this.storageName));

        verify(this.storageService).getStorageByName(this.storageName);
    }

    @Test
    public void verifyValidGetAllStorages() throws Exception {
        when(this.storageService.getAllStorages())
                .thenReturn(this.storageList);

        this.mockMvc.perform(get("/storage/storages")
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(this.storageList.size())));

        verify(this.storageService).getAllStorages();
    }

    @Test
    public void verifyValidAddStorage() throws Exception {
        when(this.storageService.addStorage(argThat(new AnyStorage())))
                .thenReturn(this.storage);

        this.mockMvc.perform(post("/storage/storage")
                .contentType(MediaType.APPLICATION_JSON)
                .content(this.jsonStorage)
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.storageName").exists())
                .andExpect(jsonPath("$.storageName").value(this.storageName));

        verify(this.storageService).addStorage(argThat(new AnyStorage()));
    }

    @Test
    public void verifyValidUpdateStorage() throws Exception {
        when(this.storageService.updateStorage(argThat(new AnyStorage())))
                .thenReturn(this.storage);

        this.mockMvc.perform(put("/storage/storage")
                .contentType(MediaType.APPLICATION_JSON)
                .content(this.jsonStorage)
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.storageName").exists())
                .andExpect(jsonPath("$.storageName").value(this.storageName));

        verify(this.storageService).updateStorage(argThat(new AnyStorage()));
    }

    @Test
    public void verifyValidDeleteStorage() throws Exception {
        final String expectedResult = "Deactivated " + this.storageId;

        when(this.storageService.deleteStorage(this.storageId))
                .thenReturn(this.storage);

        this.mockMvc.perform(delete("/storage/dstorage/{id}", this.storageId)
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString(expectedResult)));

        verify(this.storageService).deleteStorage(this.storageId);
    }

    @Test
    public void verifyValidEnableStorage() throws Exception {
        final String expectedResult = "Enabled " + this.storageId;

        when(this.storageService.enableStorage(this.storageId))
                .thenReturn(this.storage);

        this.mockMvc.perform(put("/storage/estorage/{id}", this.storageId)
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString(expectedResult)));

        verify(this.storageService).enableStorage(this.storageId);
    }
}
