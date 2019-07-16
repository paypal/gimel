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
import java.util.ArrayList;
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
import com.paypal.udc.dao.storagesystem.StorageSystemDiscoveryRepository;
import com.paypal.udc.entity.storagesystem.StorageSystem;
import com.paypal.udc.entity.storagesystem.StorageSystemAttributeValue;
import com.paypal.udc.entity.storagesystem.StorageSystemDiscovery;
import com.paypal.udc.service.IStorageSystemService;
import com.paypal.udc.util.StorageSystemUtil;
import com.paypal.udc.util.enumeration.RunFrequencyEnumeration;


@RunWith(SpringRunner.class)
@WebMvcTest(StorageSystemController.class)
public class StorageSystemControllerTest {

    private MockMvc mockMvc;

    @MockBean
    private IStorageSystemService storageSystemService;

    @MockBean
    private StorageSystemDiscoveryRepository ssdr;

    @MockBean
    private StorageSystemUtil storageSystemUtil;

    @Autowired
    private WebApplicationContext webApplicationContext;

    final Gson gson = new Gson();

    private StorageSystem storageSystem;
    private StorageSystemDiscovery storageSystemDiscovery;
    private Long storageSystemId;
    private long systemId;
    private String systemIds;
    private Long storageSystemDiscoveryId;
    private String storageSystemName;
    private Long storageTypeId;
    private Long clusterId;
    private Long zoneId;
    private Long entityId;
    private List<StorageSystemDiscovery> discoveryList;
    private StorageSystemAttributeValue storageAttribute;
    private List<StorageSystemAttributeValue> storageAttributesList;

    private List<StorageSystem> storageSystemsList;

    private String storageTypeName;

    private String jsonStorageSystem;
    private String jsonStorageSystemDiscovery;

    class AnyStorageSystem implements ArgumentMatcher<StorageSystem> {
        @Override
        public boolean matches(final StorageSystem storageSystem) {
            return storageSystem instanceof StorageSystem;
        }
    }

    class AnyStorageSystemDiscovery implements ArgumentMatcher<StorageSystemDiscovery> {
        @Override
        public boolean matches(final StorageSystemDiscovery storageSystemDiscovery) {
            return storageSystemDiscovery instanceof StorageSystemDiscovery;
        }
    }

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        this.mockMvc = MockMvcBuilders.webAppContextSetup(this.webApplicationContext).build();
        this.discoveryList = new ArrayList<StorageSystemDiscovery>();
        this.storageSystemDiscoveryId = 0L;
        this.storageSystemId = 1L;
        this.systemId = 1;
        this.storageSystemName = "storageSystemName";
        this.storageTypeId = 2L;
        this.clusterId = 1L;
        this.zoneId = 1L;
        this.entityId = 1L;
        this.systemIds = Long.toString(this.systemId);
        this.storageSystem = new StorageSystem(this.storageSystemId, this.storageSystemName, "storageSystemDescription",
                "crUser", "crTime", "updUser", "updTime", this.storageTypeId, this.clusterId, this.zoneId,
                this.entityId, "Y",
                RunFrequencyEnumeration.SIXTY.getFlag());
        this.storageSystemDiscovery = new StorageSystemDiscovery(this.storageSystemId, "2019-01-02 22:38:20",
                "2019-01-02 22:48:20", 2, 5, 20, "SUCCESS", "", "udc_admin");
        this.discoveryList.add(this.storageSystemDiscovery);
        this.storageAttribute = new StorageSystemAttributeValue();
        this.storageAttributesList = Arrays.asList(this.storageAttribute);
        this.storageSystemsList = Arrays.asList(this.storageSystem);
        this.storageTypeName = "storageTypeName";
        this.jsonStorageSystemDiscovery = "{"
                + "\"storageSystemDiscoveryId\": 0,"
                + "\"storageSystemId\": 1,"
                + "\"startTime\": \"2019-01-02 22:38:20.0\","
                + "\"endTime\": \"2019-01-02 22:48:20.0\","
                + "\"totalUpserts\": 5,\"totalInserts\": 20,"
                + "\"discoveryStatus\": \"SUCCESS\","
                + "\"errorLog\": \"\","
                + "\"runningUser\": \"udc_admin\""
                + "}";
        this.jsonStorageSystem = "{ " +
                "\"adminUserId\": 0, " +
                "\"containers\": \"string\", " +
                "\"createdTimestamp\": \"string\", " +
                "\"createdUser\": \"string\", " +
                "\"isActiveYN\": \"string\", " +
                "\"storageSystemDescription\": \"string\", " +
                "\"storageSystemId\": 0, " +
                "\"storageSystemName\": \"string\", " +
                "\"storageType\": { " +
                "\"attributeKeys\": [ " +
                "{ " +
                "\"createdTimestamp\": \"string\", " +
                "\"createdUser\": \"string\", " +
                "\"isActiveYN\": \"string\", " +
                "\"isStorageSystemLevel\": \"string\", " +
                "\"storageDsAttributeKeyDesc\": \"string\", " +
                "\"storageDsAttributeKeyId\": 0, " +
                "\"storageDsAttributeKeyName\": \"string\", " +
                "\"storageTypeAttributeValue\": \"string\", " +
                "\"storageTypeId\": 0, " +
                "\"updatedTimestamp\": \"string\", " +
                "\"updatedUser\": \"string\" " +
                "} " +
                "], " +
                "\"createdUser\": \"string\", " +
                "\"isActiveYN\": \"string\", " +
                "\"storage\": { " +
                "\"createdTimestamp\": \"string\", " +
                "\"createdUser\": \"string\", " +
                "\"isActiveYN\": \"string\", " +
                "\"storageDescription\": \"string\", " +
                "\"storageId\": 0, " +
                "\"storageName\": \"string\", " +
                "\"updatedUser\": \"string\" " +
                "}, " +
                "\"storageId\": 0, " +
                "\"storageTypeDescription\": \"string\", " +
                "\"storageTypeId\": 0, " +
                "\"storageTypeName\": \"string\", " +
                "\"updatedUser\": \"string\" " +
                "}, " +
                "\"storageTypeId\": 0, " +
                "\"systemAttributeValues\": [ " +
                "{ " +
                "\"createdTimestamp\": \"string\", " +
                "\"createdUser\": \"string\", " +
                "\"storageDataSetAttributeKeyId\": 0, " +
                "\"storageDsAttributeKeyName\": \"string\", " +
                "\"storageSystemAttributeValue\": \"string\", " +
                "\"storageSystemAttributeValueId\": 0, " +
                "\"storageSystemID\": 0, " +
                "\"updatedTimestamp\": \"string\", " +
                "\"updatedUser\": \"string\" " +
                "} " +
                "], " +
                "\"updatedTimestamp\": \"string\", " +
                "\"updatedUser\": \"string\" " +
                "}";
    }

    @Test
    public void verifyValidGetStorageSystemById() throws Exception {
        when(this.storageSystemService.getStorageSystemById(this.storageSystemId))
                .thenReturn(this.storageSystem);

        this.mockMvc.perform(get("/storageSystem/storageSystem/{id}", this.storageSystemId)
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.storageSystemId").value(this.storageSystemId));

        verify(this.storageSystemService).getStorageSystemById(this.storageSystemId);
    }

    @Test
    public void verifyValidGetStorageSystemDiscoveryById() throws Exception {
        when(this.storageSystemService.getDiscoveryStatusForStorageSystemId(this.systemIds))
                .thenReturn(this.discoveryList);

        this.mockMvc.perform(get("/storageSystem/storageSystemDiscovery/{id}", this.systemId)
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(this.discoveryList.size())));

        verify(this.storageSystemService).getDiscoveryStatusForStorageSystemId(this.systemIds);
    }

    @Test
    public void verifyValidGetStorageAttributesById() throws Exception {
        when(this.storageSystemService.getStorageSystemAttributes(this.storageSystemId))
                .thenReturn(this.storageAttributesList);

        this.mockMvc.perform(get("/storageSystem/storageSystemAttributes/{id}", this.storageSystemId)
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(this.storageAttributesList.size())));

        verify(this.storageSystemService).getStorageSystemAttributes(this.storageSystemId);
    }

    @Test
    public void verifyValidGetStorageAttributesByName() throws Exception {
        when(this.storageSystemService.getAttributeValuesByName(this.storageSystemName))
                .thenReturn(this.storageAttributesList);

        this.mockMvc.perform(
                get("/storageSystem/storageSystemAttributesByName/{storageSystemName:.+}", this.storageSystemName)
                        .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(this.storageAttributesList.size())));

        verify(this.storageSystemService).getAttributeValuesByName(this.storageSystemName);
    }

    @Test
    public void verifyValidGetStorageSystemByType() throws Exception {
        when(this.storageSystemService.getStorageSystemByStorageType(this.storageTypeId))
                .thenReturn(this.storageSystemsList);

        this.mockMvc.perform(
                get("/storageSystem/storageSystemByType/{storageTypeId}", this.storageTypeId)
                        .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(this.storageSystemsList.size())));

        verify(this.storageSystemService).getStorageSystemByStorageType(this.storageTypeId);
    }

    @Test
    public void verifyValidGetStorageSystemByTypeName() throws Exception {
        when(this.storageSystemService.getStorageSystemByType(this.storageTypeName))
                .thenReturn(this.storageSystemsList);

        this.mockMvc.perform(
                get("/storageSystem/storageSystemByTypeName/{storageTypeName:.+}", this.storageTypeName)
                        .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(this.storageSystemsList.size())));

        verify(this.storageSystemService).getStorageSystemByType(this.storageTypeName);
    }

    @Test
    public void verifyValidGetAllStorageSystems() throws Exception {
        when(this.storageSystemService.getAllStorageSystems())
                .thenReturn(this.storageSystemsList);

        this.mockMvc.perform(get("/storageSystem/storageSystems")
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(this.storageSystemsList.size())));

        verify(this.storageSystemService).getAllStorageSystems();
    }

    @Test
    public void verifyValidAddStorageSystem() throws Exception {
        when(this.storageSystemService.addStorageSystem(argThat(new AnyStorageSystem())))
                .thenReturn(this.storageSystem);

        this.mockMvc.perform(post("/storageSystem/storageSystem")
                .contentType(MediaType.APPLICATION_JSON)
                .content(this.jsonStorageSystem)
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.storageSystemId").exists())
                .andExpect(jsonPath("$.storageSystemId").value(this.storageSystemId));

        verify(this.storageSystemService).addStorageSystem(argThat(new AnyStorageSystem()));
    }

    @Test
    public void verifyValidAddStorageSystemDiscovery() throws Exception {
        when(this.storageSystemService.addStorageSystemDiscovery(argThat(new AnyStorageSystemDiscovery())))
                .thenReturn(this.storageSystemDiscovery);

        this.mockMvc.perform(post("/storageSystem/storageSystemDiscovery")
                .contentType(MediaType.APPLICATION_JSON)
                .content(this.jsonStorageSystemDiscovery)
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.storageSystemDiscoveryId").exists())
                .andExpect(jsonPath("$.storageSystemDiscoveryId").value(this.storageSystemDiscoveryId));

        verify(this.storageSystemService).addStorageSystemDiscovery(argThat(new AnyStorageSystemDiscovery()));

    }

    @Test
    public void verifyValidUpdateStorageSystem() throws Exception {
        when(this.storageSystemService.updateStorageSystem(argThat(new AnyStorageSystem())))
                .thenReturn(this.storageSystem);

        this.mockMvc.perform(put("/storageSystem/storageSystem")
                .contentType(MediaType.APPLICATION_JSON)
                .content(this.jsonStorageSystem)
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.storageSystemId").exists())
                .andExpect(jsonPath("$.storageSystemId").value(this.storageSystemId));

        verify(this.storageSystemService).updateStorageSystem(argThat(new AnyStorageSystem()));
    }

    @Test
    public void verifyValidDeleteStorageSystem() throws Exception {
        final String expectedResult = "Deactivated " + this.storageSystemId;

        when(this.storageSystemService.deleteStorageSystem(this.storageSystemId))
                .thenReturn(this.storageSystem);

        this.mockMvc.perform(delete("/storageSystem/dstorageSystem/{id}", this.storageSystemId)
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString(expectedResult)));

        verify(this.storageSystemService).deleteStorageSystem(this.storageSystemId);
    }

    @Test
    public void verifyValidEnableStorageSystem() throws Exception {
        final String expectedResult = "Reactivated " + this.storageSystemId;

        when(this.storageSystemService.enableStorageSystem(this.storageSystemId))
                .thenReturn(this.storageSystem);

        this.mockMvc.perform(put("/storageSystem/estorageSystem/{id}", this.storageSystemId)
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString(expectedResult)));

        verify(this.storageSystemService).enableStorageSystem(this.storageSystemId);
    }
}
