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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.test.context.junit4.SpringRunner;
import com.paypal.udc.dao.cluster.ClusterRepository;
import com.paypal.udc.dao.dataset.DatasetChangeLogRepository;
//** TODO: Remove Commented Code **
//import com.paypal.udc.dao.dataset.DatasetChangeLogRegisteredRepository;
import com.paypal.udc.dao.dataset.DatasetRepository;
import com.paypal.udc.dao.dataset.DatasetStorageSystemRepository;
import com.paypal.udc.dao.objectschema.ObjectSchemaAttributeValueRepository;
import com.paypal.udc.dao.objectschema.ObjectSchemaMapRepository;
import com.paypal.udc.dao.storagesystem.StorageSystemAttributeValueRepository;
import com.paypal.udc.dao.storagesystem.StorageSystemRepository;
import com.paypal.udc.dao.storagetype.StorageTypeRepository;
import com.paypal.udc.entity.cluster.Cluster;
import com.paypal.udc.entity.dataset.Dataset;
import com.paypal.udc.entity.dataset.DatasetChangeLog;
import com.paypal.udc.entity.storagesystem.StorageSystem;
import com.paypal.udc.entity.storagetype.StorageType;
import com.paypal.udc.util.ClusterUtil;
import com.paypal.udc.util.DatasetUtil;
import com.paypal.udc.util.ObjectSchemaMapUtil;
import com.paypal.udc.util.StorageSystemUtil;
import com.paypal.udc.util.StorageTypeUtil;
import com.paypal.udc.util.UserUtil;
import com.paypal.udc.validator.dataset.DatasetAliasValidator;
import com.paypal.udc.validator.dataset.DatasetDescValidator;
import com.paypal.udc.validator.dataset.DatasetNameValidator;


@RunWith(SpringRunner.class)
public class DatasetServiceTest {

    @Mock
    private StorageSystemUtil systemUtil;

    @Mock
    private DatasetRepository dataSetRepository;

    @Mock
    private UserUtil userUtil;

    @Mock
    private ClusterUtil clusterUtil;

    @Mock
    private StorageSystemAttributeValueRepository systemAttributeValueRepository;

    @Mock
    private DatasetStorageSystemRepository datasetStorageSystemRepository;

    @Mock
    private StorageSystemRepository storageSystemRepository;

    @Mock
    private DatasetUtil dataSetUtil;

    @Mock
    private StorageTypeUtil storageTypeUtil;

    @Mock
    private StorageTypeRepository storageTypeRepository;

    @Mock
    private DatasetNameValidator s1;

    @Mock
    private DatasetDescValidator s2;

    @Mock
    private DatasetAliasValidator s4;

    // ** TODO: Remove Commented Code **
    // @Mock
    // private DatasetChangeLogRegisteredRepository changeLogRegisteredRepository;

    // ** Change **
    @Mock
    private DatasetChangeLogRepository changeLogRepository;

    @Mock
    private ObjectSchemaMapRepository objectSchemaMapRepository;

    @Mock
    private ObjectSchemaAttributeValueRepository objectSchemaAttributeRepository;

    // @Mock
    // private ObjectSchemaRegistrationMapRepository objectSchemaClusterRepository;

    @Mock
    private ClusterRepository clusterRepository;

    @Mock
    private ObjectSchemaMapUtil schemaMapUtil;

    @InjectMocks
    private DatasetService datasetService;

    private String datasetSubstring;
    private String storageTypeName;
    private String zoneName;
    private StorageType storageType;
    private StorageSystem storageSystem;
    private List<StorageSystem> storageSystems;
    private String storageSystemName;
    private Pageable pageable;
    private Page<Dataset> pageDataset;
    private Long dataSetId = 3L;
    private Dataset dataSet = new Dataset();
    private Map<Long, StorageSystem> systemMappings = new HashMap<Long, StorageSystem>();
    private List<Cluster> clusterList = new ArrayList<Cluster>();
    private Cluster cluster = new Cluster();
    private Long storageClusterId;
    private DatasetChangeLog changeLogByDataset;
    private List<DatasetChangeLog> changeLogsByDataset;
    private DatasetChangeLog changeLogByDatasetAndChangeColumn;
    private List<DatasetChangeLog> changeLogsByDatasetAndChangeColumn;
    private String changeType;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        this.datasetSubstring = "dataset";
        this.storageTypeName = "All";
        this.storageType = new StorageType();
        this.storageType.setStorageTypeId(0L);
        this.storageSystem = new StorageSystem();
        this.storageSystem.setStorageSystemId(0L);
        this.storageSystems = Arrays.asList(this.storageSystem);
        this.storageSystemName = "All";
        this.zoneName = "All";
        this.cluster.setClusterId(5L);
        this.clusterList.add(this.cluster);
        this.dataSet.setStorageSystemId(4L);
        this.dataSet.setStorageSystemName(this.storageSystemName);
        this.systemMappings.put(4L, this.storageSystem);
        this.storageClusterId = 6L;
        this.changeType = "DATASET";
        this.changeLogByDataset = new DatasetChangeLog(this.dataSetId, "C", "DATASET", "{}",
                "{\"value\": \"test1.udc1.testingOld\", \"username\": \"nighosh\"}", "2019-04-02 10:33:21");
        this.changeLogsByDataset = Arrays.asList(this.changeLogByDataset);
        this.changeLogByDatasetAndChangeColumn = new DatasetChangeLog(this.dataSetId, "M", "DATASET",
                "{\"value\": \"test1.udc1.testingOld\", \"username\": \"nighosh\"}",
                "{\"value\": \"test1.udc1.testingNew\", \"username\": \"nighosh\"}", "2019-04-02 10:33:25");
        this.changeLogsByDatasetAndChangeColumn = Arrays.asList(this.changeLogByDatasetAndChangeColumn);

    }

    @Test
    public void verifyValidGetAllDatasetsByTypeAndSystem() throws Exception {
        when(this.dataSetUtil.getDatasetsForAllTypesAndSystems(this.datasetSubstring, this.pageable))
                .thenReturn(this.pageDataset);

        final Page<Dataset> result = this.datasetService.getAllDatasetsByTypeAndSystem(this.datasetSubstring,
                this.storageTypeName, this.storageSystemName, this.zoneName, this.pageable);
        assertEquals(this.pageDataset, result);

        verify(this.dataSetUtil).getDatasetsForAllTypesAndSystems(this.datasetSubstring, this.pageable);
    }

    @Test
    public void verifyValidGetAllDeletedDatasetsByTypeAndSystem() throws Exception {
        when(this.dataSetUtil.getDeletedDatasetsForAllTypesAndSystems(this.datasetSubstring, this.pageable))
                .thenReturn(this.pageDataset);

        final Page<Dataset> result = this.datasetService.getAllDeletedDatasetsByTypeAndSystem(this.datasetSubstring,
                this.storageTypeName, this.storageSystemName, this.pageable);
        assertEquals(this.pageDataset, result);

        verify(this.dataSetUtil).getDeletedDatasetsForAllTypesAndSystems(this.datasetSubstring, this.pageable);
    }

    @Test
    public void verifyValidGetPendingDatasets() throws Exception {
        when(this.dataSetUtil.getPendingDatasetsForAllTypesAndSystems(this.datasetSubstring, this.pageable))
                .thenReturn(this.pageDataset);

        final Page<Dataset> result = this.datasetService.getPendingDatasets(this.datasetSubstring,
                this.storageTypeName, this.storageSystemName, this.pageable);
        assertEquals(this.pageDataset, result);

        verify(this.dataSetUtil).getPendingDatasetsForAllTypesAndSystems(this.datasetSubstring, this.pageable);
    }

    @Test
    public void verifyValidGetDataSetById() throws Exception {
        when(this.dataSetRepository.findById(this.dataSetId)).thenReturn(Optional.of(this.dataSet));
        when(this.systemUtil.getStorageSystems()).thenReturn(this.systemMappings);
        when(this.clusterRepository.findAll()).thenReturn(this.clusterList);
        when(this.dataSetUtil.getDataSet(this.dataSet)).thenReturn(this.dataSet);

        final Dataset result = this.datasetService.getDataSetById(this.dataSetId);
        assertEquals(this.dataSet, result);

        verify(this.clusterRepository).findAll();
    }

    @Test
    public void verifyValidGetAllChangeLogsByDataSetId() throws Exception {
        when(this.changeLogRepository.findByStorageDataSetId(this.dataSetId))
                .thenReturn(this.changeLogsByDataset);

        final List<DatasetChangeLog> result = this.datasetService.getChangeLogsByDataSetId(this.dataSetId);
        assertEquals(this.changeLogsByDataset, result);

        verify(this.changeLogRepository).findByStorageDataSetId(this.dataSetId);
    }

    @Test
    public void verifyValidGetAllChangeLogsByDataSetIdAndChangeColumnType() throws Exception {
        when(this.changeLogRepository.findByStorageDataSetIdAndChangeColumnType(this.dataSetId, this.changeType))
                .thenReturn(this.changeLogsByDatasetAndChangeColumn);

        final List<DatasetChangeLog> result = this.datasetService.getChangeLogsByDataSetIdAndChangeColumnType(
                this.dataSetId,
                this.changeType);
        assertEquals(this.changeLogsByDatasetAndChangeColumn, result);

        verify(this.changeLogRepository).findByStorageDataSetIdAndChangeColumnType(this.dataSetId, this.changeType);
    }

}
