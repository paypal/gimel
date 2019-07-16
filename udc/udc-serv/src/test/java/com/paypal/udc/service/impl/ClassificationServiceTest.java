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

package com.paypal.udc.service.impl;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.test.context.junit4.SpringRunner;
import com.paypal.udc.dao.classification.ClassificationRepository;
import com.paypal.udc.dao.classification.PageableClassificationRepository;
import com.paypal.udc.dao.integration.common.SourceProviderRepository;
import com.paypal.udc.entity.classification.Classification;
import com.paypal.udc.entity.integration.common.SourceProvider;
import com.paypal.udc.util.ClassificationUtil;


@RunWith(SpringRunner.class)
public class ClassificationServiceTest {

    @Mock
    private PageableClassificationRepository pageClassificationRepository;

    @Mock
    private ClassificationRepository classificationDatasetRepository;

    @InjectMocks
    private ClassificationDatasetService classificationDatasetService;

    @Mock
    private ClassificationUtil classificationUtil;

    @Mock
    private SourceProviderRepository providerRepository;

    private Classification classification;
    private Long datasetClassificationId, classificationId, providerId;
    private String classificationComment, objectName, columnName, providerName, dataSource, dataSourceType,
            containerName, createdUser,
            createdTimestamp,
            updatedUser,
            updatedTimestamp;
    private List<Classification> classifications;
    private Pageable pageable;
    private Page<Classification> pagedClassifications;
    private SourceProvider provider;
    private List<SourceProvider> providerList;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        this.datasetClassificationId = 1L;
        this.objectName = "ObjectName";
        this.columnName = "ColumnName";
        this.providerId = 1L;
        this.providerName = "Provider";
        this.classificationId = 3L;
        this.classificationComment = "Classification Comment";
        this.dataSource = "DataSource";
        this.dataSourceType = "DataSourceType";
        this.containerName = "ContainerName";
        this.createdUser = "crUser";
        this.createdTimestamp = "crTime";
        this.updatedUser = "updUser";
        this.updatedTimestamp = "updTime";
        this.classification = new Classification(this.datasetClassificationId,
                this.objectName, this.columnName, this.providerId, this.classificationId, this.classificationComment,
                this.dataSource, this.dataSourceType, this.containerName,
                this.createdUser, this.createdTimestamp, this.updatedUser,
                this.updatedTimestamp);
        // this.deletedClassification = new Classification(this.classificationMapId,
        // this.storageSystemId,
        // this.objectName, this.columnName, this.provider, this.classificationId, this.classificationComment,
        // this.sampleSize,
        // this.createdUser, this.createdTimestamp, this.updatedUser,
        // this.updatedTimestamp);
        this.classification.setProviderName(this.providerName);
        this.classifications = Arrays.asList(this.classification);
        this.pageable = new PageRequest(0, 3);
        this.provider = new SourceProvider(this.providerName, "Description", "CrUser",
                "CrTime", "UpdUser", "UpdTime");
        this.provider.setSourceProviderId(this.providerId);
        this.providerList = Arrays.asList(this.provider);
    }

    @Test
    public void verifyValidGetAllClassifications() throws Exception {
        when(this.providerRepository.findAll()).thenReturn(this.providerList);
        when(this.pageClassificationRepository.findAll(this.pageable)).thenReturn(this.pagedClassifications);

        final Page<Classification> result = this.classificationDatasetService
                .getAllClassifications(this.pageable);
        assertEquals(this.pagedClassifications, result);

        verify(this.pageClassificationRepository).findAll(this.pageable);
    }

    @Test
    public void verifyValidGetClassificationsByClassificationId() throws Exception {
        when(this.providerRepository.findAll()).thenReturn(this.providerList);
        when(this.pageClassificationRepository.findByClassificationId(this.pageable, this.classificationId))
                .thenReturn(this.pagedClassifications);

        final Page<Classification> result = this.classificationDatasetService
                .getClassificationByClassId(this.pageable, this.classificationId);
        assertEquals(this.pagedClassifications, result);

        verify(this.pageClassificationRepository).findByClassificationId(this.pageable, this.classificationId);
    }

    @Test
    public void verifyValidGetStorageByObjectName() throws Exception {
        when(this.classificationDatasetRepository.findByObjectName(this.objectName)).thenReturn(this.classifications);

        final List<Classification> result = this.classificationDatasetService
                .getClassificationByObjectName(this.objectName);
        assertEquals(this.classifications.size(), result.size());

        verify(this.classificationDatasetRepository).findByObjectName(this.objectName);
    }

    @Test
    public void verifyValidGetStorageByColumnName() throws Exception {
        when(this.classificationDatasetRepository.findByColumnName(this.columnName)).thenReturn(this.classifications);

        final List<Classification> result = this.classificationDatasetService
                .getClassificationByColumnName(this.columnName);
        assertEquals(this.classifications.size(), result.size());

        verify(this.classificationDatasetRepository).findByColumnName(this.columnName);
    }

    @Test
    public void verifyValidAddClassification() throws Exception {
        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss");
        when(this.classificationUtil.addClassification(this.classification, sdf)).thenReturn(this.classification);
        when(this.classificationDatasetRepository.save(this.classification)).thenReturn(this.classification);
        final Classification result = this.classificationDatasetService
                .addClassification(this.classification);
        assertEquals(this.classification, result);

    }

    @Test
    public void verifyValidEditClassification() throws Exception {
        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss");
        when(this.classificationUtil.editClassification(this.classification, sdf)).thenReturn(this.classification);
        when(this.classificationDatasetRepository.save(this.classification)).thenReturn(this.classification);
        final Classification result = this.classificationDatasetService
                .editClassification(this.classification);
        assertEquals(this.classification, result);

    }

}
