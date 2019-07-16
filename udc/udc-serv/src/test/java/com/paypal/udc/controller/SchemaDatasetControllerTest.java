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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import java.util.ArrayList;
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
import com.paypal.udc.entity.integration.common.Tag;
import com.paypal.udc.entity.integration.schema.SchemaDatasetMap;
import com.paypal.udc.service.ISchemaDatasetService;


@RunWith(SpringRunner.class)
@WebMvcTest(SchemaDatasetMapController.class)
public class SchemaDatasetControllerTest {

    private MockMvc mockMvc;

    @MockBean
    private ISchemaDatasetService schemaDatasetMapService;

    @Autowired
    private WebApplicationContext webApplicationContext;

    final Gson gson = new Gson();

    private long storageSystemId, storageDatasetId, providerId;
    private String objectType, objectName, containerName, objectComment, tagName, providerName;
    private SchemaDatasetMap schema;
    private List<SchemaDatasetMap> schemas;
    private Tag tag;
    private List<Tag> tags;

    class AnySchemaDatasetMap implements ArgumentMatcher<SchemaDatasetMap> {
        @Override
        public boolean matches(final SchemaDatasetMap schemaDatasetMap) {
            return schemaDatasetMap instanceof SchemaDatasetMap;
        }
    }

    @Before
    public void setup() {

        MockitoAnnotations.initMocks(this);
        this.mockMvc = MockMvcBuilders.webAppContextSetup(this.webApplicationContext).build();
        this.storageSystemId = 1L;
        this.storageDatasetId = 1L;
        this.tagName = "tag1";
        this.providerId = 1L;
        this.providerName = "USER";
        this.objectComment = "Object Comment";
        this.containerName = "Container Name";
        this.objectName = "Object Name";
        this.objectType = "TABLE";
        this.tags = new ArrayList<Tag>();
        this.schemas = new ArrayList<SchemaDatasetMap>();
        this.schema = new SchemaDatasetMap(this.storageSystemId, this.storageDatasetId, this.providerId,
                this.objectType, this.objectName, this.containerName, this.objectComment, "CrUser", "CrTime",
                "UdpUser", "UpdTime");
        this.schemas.add(this.schema);
        this.tag = new Tag(this.tagName, this.providerId, "CrUser", "CrTime", "UdpUser", "UpdTime");
        this.tags.add(this.tag);

    }

    @Test
    public void verifyGetSchemasByDatasetId() throws Exception {
        when(this.schemaDatasetMapService.getDatasetSchemasByDatasetId(this.storageDatasetId))
                .thenReturn(this.schemas);

        this.mockMvc.perform(get("/schemaIntegration/schema/{id}", this.storageDatasetId)
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(this.schemas.size())));

        verify(this.schemaDatasetMapService).getDatasetSchemasByDatasetId(this.storageDatasetId);
    }

    @Test
    public void verifyGetSchemasBySystemObjectAndContainer() throws Exception {
        when(this.schemaDatasetMapService.getDatasetSchemaBySystemObjectAndContainerAndProviderName(
                this.storageSystemId, this.containerName, this.objectName, this.providerName))
                        .thenReturn(this.schema);

        this.mockMvc
                .perform(get("/schemaIntegration/schema/{id}/{containerName}/{objectName}/{providerName}",
                        this.storageSystemId, this.containerName, this.objectName, this.providerName)
                                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.objectName").value(this.objectName));

        verify(this.schemaDatasetMapService).getDatasetSchemaBySystemObjectAndContainerAndProviderName(
                this.storageSystemId, this.containerName, this.objectName, this.providerName);
    }

}
