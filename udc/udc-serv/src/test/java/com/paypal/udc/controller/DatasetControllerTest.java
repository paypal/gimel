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
import com.paypal.udc.entity.dataset.CumulativeDataset;
import com.paypal.udc.entity.dataset.Dataset;
import com.paypal.udc.entity.dataset.DatasetChangeLog;
import com.paypal.udc.entity.dataset.DatasetWithAttributes;
import com.paypal.udc.service.IDatasetService;


@RunWith(SpringRunner.class)
@WebMvcTest(DatasetController.class)
public class DatasetControllerTest {

    private MockMvc mockMvc;

    @MockBean
    private IDatasetService dataSetService;

    @Autowired
    private WebApplicationContext webApplicationContext;

    final Gson gson = new Gson();

    private Long storageDataSetId;
    private Dataset dataset;
    private String storageDataSetName;
    private DatasetWithAttributes dataSetWithAttributes;
    private Long objectSchemaMapId;
    private Long storageSystemId;
    private String dataSetName;
    private Long clusterId;
    private DatasetChangeLog changeLogByDataset;
    private List<DatasetChangeLog> changeLogsByDataset;
    private DatasetChangeLog changeLogByDatasetAndChangeColumn;
    private List<DatasetChangeLog> changeLogsByDatasetAndChangeColumn;
    private String changeType;
    private CumulativeDataset cumulativeDataset;
    private List<CumulativeDataset> cumulativeDatasetList;
    private String dataSetSubString;
    private String jsonDataSet;

    class AnyDataset implements ArgumentMatcher<Dataset> {
        @Override
        public boolean matches(final Dataset dataset) {
            return dataset instanceof Dataset;
        }
    }

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        this.mockMvc = MockMvcBuilders.webAppContextSetup(this.webApplicationContext).build();

        this.storageDataSetId = 0L;
        this.storageSystemId = 3L;
        this.storageDataSetName = "storageDataSetName";
        this.dataset = new Dataset(this.storageDataSetName, this.storageSystemId, "storageDataSetAliasName",
                "storageContainerName", "storageDatabaseName", "storageDataSetDescription", "Y", "crUser", "crTime",
                "updUser", "updTime", 1L, "isAutoRegistered");
        this.objectSchemaMapId = 2L;

        this.dataSetWithAttributes = new DatasetWithAttributes(this.storageDataSetId, this.storageDataSetName,
                this.objectSchemaMapId, "storageDataSetAliasName", "isAutoRegistered", "crUser", "crTime", "updUser",
                "updTime", this.storageSystemId, "query", "Y", "storageSystemName", "", "");
        this.dataSetName = "dataSetName";
        this.clusterId = 4L;
        this.changeLogByDataset = new DatasetChangeLog(this.storageDataSetId, "C", "DATASET", "{}",
                "{\"value\": \"test1.udc1.testingOld\", \"username\": \"nighosh\"}", "2019-04-02 10:33:21");
        this.changeLogsByDataset = Arrays.asList(this.changeLogByDataset);
        this.changeLogByDatasetAndChangeColumn = new DatasetChangeLog(this.storageDataSetId, "M", "DATASET",
                "{\"value\": \"test1.udc1.testingOld\", \"username\": \"nighosh\"}",
                "{\"value\": \"test1.udc1.testingNew\", \"username\": \"nighosh\"}", "2019-04-02 10:33:25");
        this.changeLogsByDatasetAndChangeColumn = Arrays.asList(this.changeLogByDatasetAndChangeColumn);
        this.dataSetSubString = "dataSetSubString";
        this.changeType = "DATASET";
        this.cumulativeDatasetList = Arrays.asList(this.cumulativeDataset);
        this.jsonDataSet = "{ " +
                "\"attributesPresent\": true, " +
                "\"clusterNames\": [ " +
                "{ " +
                "\"clusterId\": 0, " +
                "\"clusterName\": \"string\", " +
                "\"deploymentStatus\": \"string\" " +
                "} " +
                "], " +
                "\"createdTimestamp\": \"string\", " +
                "\"createdUser\": \"string\", " +
                "\"isActiveYN\": \"string\", " +
                "\"isAutoRegistered\": \"string\", " +
                "\"objectSchemaMapId\": 0, " +
                "\"storageDataSetAliasName\": \"string\", " +
                "\"storageDataSetDescription\": \"string\", " +
                "\"storageDataSetId\": 0, " +
                "\"storageDataSetName\": \"string\", " +
                "\"storageDatabaseName\": \"string\", " +
                "\"storageSystemId\": 0, " +
                "\"storageSystemName\": \"string\", " +
                "\"updatedTimestamp\": \"string\", " +
                "\"updatedUser\": \"string\", " +
                "\"userId\": 0 " +
                "} ";
    }

    @Test
    public void verifyValidGetDataSetById() throws Exception {
        when(this.dataSetService.getDataSetById(this.storageDataSetId))
                .thenReturn(this.dataset);

        this.mockMvc.perform(get("/dataSet/dataSet/{id}", this.storageDataSetId)
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.storageDataSetName").value(this.storageDataSetName));

        verify(this.dataSetService).getDataSetById(this.storageDataSetId);
    }

    @Test
    public void verifyValidGetPendingDataSetById() throws Exception {
        when(this.dataSetService.getPendingDataset(this.storageDataSetId))
                .thenReturn(this.dataSetWithAttributes);

        this.mockMvc.perform(get("/dataSet/dataSetPending/{id}", this.storageDataSetId)
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.storageDataSetName").value(this.storageDataSetName));

        verify(this.dataSetService).getPendingDataset(this.storageDataSetId);
    }

    @Test
    public void verifyValidGetDataSetByName() throws Exception {
        when(this.dataSetService.getDataSetByName(this.dataSetName))
                .thenReturn(this.dataSetWithAttributes);

        this.mockMvc.perform(get("/dataSet/dataSetByName/{dataSetName:.+}", this.dataSetName)
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.storageDataSetName").value(this.storageDataSetName));

        verify(this.dataSetService).getDataSetByName(this.dataSetName);
    }

    @Test
    public void verifyValidChangeLogsByDataSetId() throws Exception {
        when(this.dataSetService.getChangeLogsByDataSetId(this.storageDataSetId))
                .thenReturn(this.changeLogsByDataset);

        this.mockMvc.perform(get("/dataSet/changeLogs/{datasetId}", this.storageDataSetId)
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(this.changeLogsByDataset.size())));

        verify(this.dataSetService).getChangeLogsByDataSetId(this.storageDataSetId);
    }

    @Test
    public void verifyValidChangeLogsByDataSetIdAndChangeColumnType() throws Exception {
        when(this.dataSetService.getChangeLogsByDataSetIdAndChangeColumnType(this.storageDataSetId, this.changeType))
                .thenReturn(this.changeLogsByDatasetAndChangeColumn);

        this.mockMvc.perform(get("/dataSet/changeLogs/{datasetId}/{changeType}", this.storageDataSetId, this.changeType)
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(this.changeLogsByDatasetAndChangeColumn.size())));

        verify(this.dataSetService).getChangeLogsByDataSetIdAndChangeColumnType(this.storageDataSetId, this.changeType);
    }

    @Test
    public void verifyValidAddDatasetViaUDC() throws Exception {
        when(this.dataSetService.addDataset(argThat(new AnyDataset())))
                .thenReturn(this.dataset);

        this.mockMvc.perform(post("/dataSet/addDataSet")
                .contentType(MediaType.APPLICATION_JSON)
                .content(this.jsonDataSet)
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$").value(this.storageDataSetId));

        verify(this.dataSetService).addDataset(argThat(new AnyDataset()));
    }

    @Test
    public void verifyValidAddDataset() throws Exception {
        when(this.dataSetService.addDataset(argThat(new AnyDataset())))
                .thenReturn(this.dataset);

        this.mockMvc.perform(post("/dataSet/dataSet")
                .contentType(MediaType.APPLICATION_JSON)
                .content(this.jsonDataSet)
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$").value(this.storageDataSetId));

        verify(this.dataSetService).addDataset(argThat(new AnyDataset()));
    }

    @Test
    public void verifyValidUpdateDataset() throws Exception {
        when(this.dataSetService.updateDataSet(argThat(new AnyDataset())))
                .thenReturn(this.dataset);

        this.mockMvc.perform(put("/dataSet/dataSet")
                .contentType(MediaType.APPLICATION_JSON)
                .content(this.jsonDataSet)
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$").value(this.storageDataSetId));

        verify(this.dataSetService).updateDataSet(argThat(new AnyDataset()));
    }

    @Test
    public void verifyValidDeleteDataset() throws Exception {
        final String expectedResult = "Deleted " + this.storageDataSetId;

        when(this.dataSetService.deleteDataSet(this.storageDataSetId))
                .thenReturn(this.dataset);

        this.mockMvc.perform(delete("/dataSet/dataSet/{id}", this.storageDataSetId)
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString(expectedResult)));

        verify(this.dataSetService).deleteDataSet(this.storageDataSetId);
    }
}
