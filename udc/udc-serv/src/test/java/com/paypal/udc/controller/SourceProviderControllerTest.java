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
import com.paypal.udc.entity.integration.common.SourceProvider;
import com.paypal.udc.service.IProviderService;


@RunWith(SpringRunner.class)
@WebMvcTest(SourceProviderController.class)
public class SourceProviderControllerTest {

    private MockMvc mockMvc;

    @MockBean
    private IProviderService providerService;

    @Autowired
    private WebApplicationContext webApplicationContext;

    final Gson gson = new Gson();

    private long providerId, providerIdUpd;
    private String providerName, providerNameUpd;
    private SourceProvider provider, providerUpd;
    private List<SourceProvider> providerList;
    private String jsonProvider;

    class AnyProvider implements ArgumentMatcher<SourceProvider> {
        @Override
        public boolean matches(final SourceProvider sourceProvider) {
            return sourceProvider instanceof SourceProvider;
        }
    }

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        this.mockMvc = MockMvcBuilders.webAppContextSetup(this.webApplicationContext).build();

        this.providerId = 1L;
        this.providerIdUpd = 2L;
        this.providerName = "provider1";
        this.providerNameUpd = "provider2";
        this.provider = new SourceProvider(this.providerName, "Description1", "CrUser", "CrTime",
                "UdpUser", "UpdTime");
        this.provider.setSourceProviderId(this.providerId);
        this.providerUpd = new SourceProvider(this.providerNameUpd, "Description2",
                "CrUser", "CrTime", "UpdUser", "UpdTime");
        this.providerUpd.setSourceProviderId(this.providerIdUpd);
        this.providerList = Arrays.asList(this.provider, this.providerUpd);

        this.jsonProvider = "{" +
                "\"sourceProviderDescription\": \"Description1\", " +
                "\"sourceProviderId\": 1, " +
                "\"sourceProviderName\": \"provider1\", " +
                "\"createdTimestamp\": \"CrTime\", " +
                "\"createdUser\": \"CrUser\", " +
                "\"updatedUser\": \"UpdUser\"" +
                "}";
    }

    @Test
    public void verifyValidGetProviderById() throws Exception {
        when(this.providerService.getProviderById(this.providerId))
                .thenReturn(this.provider);

        this.mockMvc.perform(get("/provider/provider/{id}", this.providerId)
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.sourceProviderId").value(this.providerId));

        verify(this.providerService).getProviderById(this.providerId);
    }

    @Test
    public void verifyValidGetProviderByName() throws Exception {
        when(this.providerService.getProviderByName(this.providerName))
                .thenReturn(this.provider);

        this.mockMvc.perform(get("/provider/providerByName/{name:.+}", this.providerName)
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.sourceProviderName").value(this.providerName));

        verify(this.providerService).getProviderByName(this.providerName);
    }

    @Test
    public void verifyValidGetAllProviders() throws Exception {
        when(this.providerService.getAllProviders())
                .thenReturn(this.providerList);

        this.mockMvc.perform(get("/provider/providers")
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(this.providerList.size())));

        verify(this.providerService).getAllProviders();
    }

    @Test
    public void verifyValidAddProvider() throws Exception {
        when(this.providerService.addProvider(argThat(new AnyProvider())))
                .thenReturn(this.provider);

        this.mockMvc.perform(post("/provider/provider")
                .contentType(MediaType.APPLICATION_JSON)
                .content(this.jsonProvider)
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.sourceProviderId").exists())
                .andExpect(jsonPath("$.sourceProviderId").value(this.providerId));

        verify(this.providerService).addProvider(argThat(new AnyProvider()));
    }

    @Test
    public void verifyValidUpdateStorage() throws Exception {
        when(this.providerService.updateProvider(argThat(new AnyProvider())))
                .thenReturn(this.provider);

        this.mockMvc.perform(put("/provider/provider")
                .contentType(MediaType.APPLICATION_JSON)
                .content(this.jsonProvider)
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.sourceProviderId").exists())
                .andExpect(jsonPath("$.sourceProviderId").value(this.providerId));

        verify(this.providerService).updateProvider(argThat(new AnyProvider()));
    }

}
