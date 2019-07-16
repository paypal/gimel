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
import com.paypal.udc.entity.integration.description.DatasetDescriptionMap;
import com.paypal.udc.entity.integration.schema.SchemaDatasetMap;
import com.paypal.udc.service.IDatasetDescriptionService;


@RunWith(SpringRunner.class)
@WebMvcTest(DatasetDescriptionMapController.class)
public class DatasetDescriptionControllerTest {

    private MockMvc mockMvc;

    @MockBean
    private IDatasetDescriptionService bodhiDatasetMapService;
    
    @Autowired
    private WebApplicationContext webApplicationContext;

    final Gson gson = new Gson();

    private long storageSystemId, storageDatasetId, providerId;
    private String objectName, containerName, objectComment;
    private DatasetDescriptionMap schema;
    private List<DatasetDescriptionMap> schemas;

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
        this.providerId = 1L;
        this.objectComment = "Object Comment";
        this.containerName = "Container Name";
        this.objectName = "Object Name";
        this.schemas = new ArrayList<DatasetDescriptionMap>();
        this.schema = new DatasetDescriptionMap(this.storageSystemId, this.providerId, this.storageDatasetId,
                this.objectName, this.containerName, this.objectComment, "CrUser", "CrTime", "UdpUser", "UpdTime");
        this.schemas.add(this.schema);
    }

    @Test
    public void verifyGetBodhiSchemasByDatasetId() throws Exception {
        when(this.bodhiDatasetMapService.getDatasetSchemasByDatasetId(this.storageDatasetId))
                .thenReturn(this.schemas);

        this.mockMvc.perform(get("/description/description/{id}", this.storageDatasetId)
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(this.schemas.size())));

        verify(this.bodhiDatasetMapService).getDatasetSchemasByDatasetId(this.storageDatasetId);
    }

    @Test
    public void verifyGetBodhiSchemasBySystemObjectAndContainer() throws Exception {
        when(this.bodhiDatasetMapService.getDatasetDescriptionMapBySystemContainerAndObjectAndProvider(
                this.storageSystemId, this.containerName, this.objectName, "USER"))
                        .thenReturn(this.schema);

        this.mockMvc
                .perform(get("/description/description/{id}/{containerName}/{objectName}/{providerName}",
                        this.storageSystemId, this.containerName, this.objectName, "USER")
                                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.objectName").value(this.objectName));

        verify(this.bodhiDatasetMapService).getDatasetDescriptionMapBySystemContainerAndObjectAndProvider(
                this.storageSystemId,
                this.containerName, this.objectName, "USER");
    }

}
