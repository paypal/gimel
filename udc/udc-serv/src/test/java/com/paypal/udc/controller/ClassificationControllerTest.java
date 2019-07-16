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
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;
import com.google.gson.Gson;
import com.paypal.udc.dao.classification.ClassificationRepository;
import com.paypal.udc.entity.classification.Classification;
import com.paypal.udc.service.IClassificationDatasetService;


@RunWith(SpringRunner.class)
@WebMvcTest(ClassificationMapController.class)
public class ClassificationControllerTest {

    private MockMvc mockMvc;

    @MockBean
    private IClassificationDatasetService classificationDatasetService;

    @MockBean
    private ClassificationRepository classificationDatasetRepository;

    @Autowired
    private WebApplicationContext webApplicationContext;

    final Gson gson = new Gson();

    private Long datasetClassificationId, classificationId, providerId;
    private String classificationComment, objectName, columnName, dataSource, dataSourceType,
            containerName, createdUser,
            createdTimestamp,
            updatedUser,
            updatedTimestamp;
    private Classification classification;
    private String jsonClassification;
    private List<Classification> datasetClassificationList;
    private Page<Classification> pageClassifications;
    private Pageable pageable;

    class AndClassification implements ArgumentMatcher<Classification> {
        @Override
        public boolean matches(final Classification classification) {
            return classification instanceof Classification;
        }
    }

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        this.mockMvc = MockMvcBuilders.webAppContextSetup(this.webApplicationContext).build();

        this.datasetClassificationId = 1L;
        this.objectName = "ObjectName";
        this.columnName = "ColumnName";
        this.providerId = 1L;
        this.classificationId = 4L;
        this.classificationComment = "Classification Comment";
        this.dataSource = "DataSource";
        this.dataSourceType = "DataSourceType";
        this.containerName = "ContainerName";
        this.createdUser = "crUser";
        this.createdTimestamp = "crTime";
        this.updatedUser = "updUser";
        this.updatedTimestamp = "updTime";
        this.pageable = new PageRequest(0, 3);
        this.classification = new Classification(this.datasetClassificationId,
                this.objectName, this.columnName, this.providerId, this.classificationId, this.classificationComment,
                this.dataSource, this.dataSourceType, this.containerName, this.createdUser, this.createdTimestamp,
                this.updatedUser, this.updatedTimestamp);
        this.datasetClassificationList = Arrays.asList(this.classification);
        this.jsonClassification = "{" +
                "\"classificationComment\": \"string\", " +
                "\"classificationId\": 3, " +
                "\"columnName\": \"ColumnName\", " +
                "\"createdTimestamp\": \"string\", " +
                "\"createdUser\": \"string\", " +
                "\"datasetClassificationId\": 1, " +
                "\"datasetIds\": [0], " +
                "\"objectName\": \"ObjectName\", " +
                "\"providerId\": 4, " +
                "\"providerName\": \"ProviderName\", " +
                "\"sampleSize\": \"string\", " +
                "\"storageSystemId\": 0, " +
                "\"updatedTimestamp\": \"string\", " +
                "\"updatedUser\": \"string\"" +
                "}";
        this.pageClassifications = new PageImpl<Classification>(
                this.datasetClassificationList, this.pageable, 1);
    }

    @Test
    public void verifyValidGetAllClassifications() throws Exception {
        when(this.classificationDatasetService.getAllClassifications(this.pageable))
                .thenReturn(this.pageClassifications);

        this.mockMvc.perform(get("/classification/classifications")
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$").exists());

    }

    @Test
    public void verifyValidGetClassificationByClassificationId() throws Exception {
        when(this.classificationDatasetService.getClassificationByClassId(this.pageable, this.classificationId))
                .thenReturn(this.pageClassifications);

        this.mockMvc.perform(get("/classification/classificationById/{id}", this.classificationId)
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$").exists());
    }

    @Test
    public void verifyValidGetClassificationByObjectAndColumn() throws Exception {
        when(this.classificationDatasetService.getClassificationByObjectAndColumn(this.objectName, this.columnName))
                .thenReturn(this.classification);

        this.mockMvc.perform(get("/classification/classificationByObjectAndColumn/{objectName}/{columnName}", this.objectName, this.columnName)
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.objectName").value(this.objectName));

        verify(this.classificationDatasetService).getClassificationByObjectAndColumn(this.objectName, this.columnName);
    }

    @Test
    public void verifyValidGetClassificationByObject() throws Exception {
        when(this.classificationDatasetService.getClassificationByObjectName(this.objectName))
                .thenReturn(this.datasetClassificationList);

        this.mockMvc.perform(get("/classification/classificationByObject/{objectName}", this.objectName)
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(this.datasetClassificationList.size())));

        verify(this.classificationDatasetService).getClassificationByObjectName(this.objectName);
    }

    @Test
    public void verifyValidGetClassificationByColumn() throws Exception {
        when(this.classificationDatasetService.getClassificationByColumnName(this.columnName))
                .thenReturn(this.datasetClassificationList);

        this.mockMvc.perform(get("/classification/classificationByColumn/{columnName}", this.columnName)
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(this.datasetClassificationList.size())));

        verify(this.classificationDatasetService).getClassificationByColumnName(this.columnName);
    }

//    @Test
//    public void verifyValidAddClassification() throws Exception {
//        when(this.classificationDatasetService.addClassification(argThat(new AndClassification())))
//                .thenReturn(this.classification);
//
//        this.mockMvc.perform(post("/classification")
//                .contentType(MediaType.APPLICATION_JSON)
//                .content(this.jsonClassification)
//                .accept(MediaType.APPLICATION_JSON_UTF8))
//                .andExpect(status().isOk())
//                .andExpect(jsonPath("$").exists())
//                .andExpect(jsonPath("$.objectName").exists())
//                .andExpect(jsonPath("$.objectName").value(this.objectName));
//
//        verify(this.classificationDatasetService).addClassification(argThat(new AndClassification()));
//    }

//    @Test
//    public void verifyValidEditClassification() throws Exception {
//        when(this.classificationDatasetService.editClassification(argThat(new AndClassification())))
//                .thenReturn(this.classification);
//
//        this.mockMvc.perform(put("/classification")
//                .contentType(MediaType.APPLICATION_JSON)
//                .content(this.jsonClassification)
//                .accept(MediaType.APPLICATION_JSON_UTF8))
//                .andExpect(status().isOk())
//                .andExpect(jsonPath("$.objectName").exists())
//                .andExpect(jsonPath("$.objectName").value(this.objectName));
//
//        verify(this.classificationDatasetService)
//                .editClassification(argThat(new AndClassification()));
//    }

    @Test
    public void verifyValidDeleteStorage() throws Exception {
        final String expectedResult = "Successfully deactivated classification";

        when(this.classificationDatasetService.deActivateClassification(this.classificationId, this.updatedUser))
                .thenReturn(this.classification);

        this.mockMvc
                .perform(put("/classification/dclassification/{id}/{updatedUser}", this.classificationId, this.updatedUser)
                        .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString(expectedResult)));

        verify(this.classificationDatasetService).deActivateClassification(this.classificationId,
                this.updatedUser);
    }

    @Test
    public void verifyValidEnableStorage() throws Exception {
        final String expectedResult = "Successfully reactivated classification";

        when(this.classificationDatasetService.reActivateClassification(this.classificationId, this.updatedUser))
                .thenReturn(this.classification);

        this.mockMvc.perform(put("/classification/eclassification/{id}/{updatedUser}", this.classificationId, this.updatedUser)
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString(expectedResult)));

        verify(this.classificationDatasetService).reActivateClassification(this.classificationId,
                this.updatedUser);
    }
}
