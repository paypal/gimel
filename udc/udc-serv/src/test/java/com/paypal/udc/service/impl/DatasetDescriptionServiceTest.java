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
import static org.mockito.Mockito.when;
import java.util.ArrayList;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;
import com.paypal.udc.dao.dataset.DatasetRepository;
import com.paypal.udc.dao.integration.description.DatasetColumnDescriptionMapRepository;
import com.paypal.udc.dao.integration.description.DatasetDescriptionMapRepository;
import com.paypal.udc.entity.integration.description.DatasetColumnDescriptionMap;
import com.paypal.udc.entity.integration.description.DatasetDescriptionMap;
import com.paypal.udc.util.DatasetDescriptionUtil;
import com.paypal.udc.util.StorageSystemUtil;


@RunWith(SpringRunner.class)
public class DatasetDescriptionServiceTest {

    @InjectMocks
    private DatasetDescriptionService bodhiDatasetService;

    @Mock
    DatasetDescriptionMapRepository bdmr;

    @Mock
    DatasetColumnDescriptionMapRepository bdcmr;

    @MockBean
    DatasetDescriptionUtil bodhiDatasetMapUtil;

    @MockBean
    StorageSystemUtil systemUtil;

    @Mock
    DatasetRepository datasetRepository;

    private long datasetId;
    private long storageSystemId;
    private long bodhiDatasetMapId;
    private long providerId;
    private DatasetDescriptionMap bodhiDatasetMap;
    private DatasetColumnDescriptionMap columnSchema;
    private String objectName, containerName, objectComment;
    private List<DatasetColumnDescriptionMap> columnSchemas;
    private List<DatasetDescriptionMap> bodhiDatasetMaps;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        this.datasetId = 1L;
        this.storageSystemId = 1L;
        this.bodhiDatasetMapId = 1L;
        this.providerId = 1L;
        this.objectComment = "Object Comment";
        this.containerName = "Container Name";
        this.objectName = "Object Name";
        this.columnSchemas = new ArrayList<DatasetColumnDescriptionMap>();
        this.bodhiDatasetMaps = new ArrayList<DatasetDescriptionMap>();
        this.bodhiDatasetMap = new DatasetDescriptionMap(this.storageSystemId, this.providerId, this.datasetId,
                this.objectName, this.containerName, this.objectComment, "CrUser", "CrTime", "UdpUser", "UpdTime");
        this.bodhiDatasetMap.setBodhiDatasetMapId(this.bodhiDatasetMapId);
        this.columnSchema = new DatasetColumnDescriptionMap(this.bodhiDatasetMapId, "columnName", "columnComment",
                "CrUser", "CrTime", "UdpUser", "UpdTime");

        this.columnSchema.setBodhiDatasetColumnMapId(1L);
        this.columnSchemas.add(this.columnSchema);
        this.bodhiDatasetMap.setColumns(this.columnSchemas);
        this.bodhiDatasetMaps.add(this.bodhiDatasetMap);

    }

    @Test
    public void verifyGetDatasetSchemasByDatasetId() throws Exception {

        when(this.bdmr.findByStorageDatasetId(this.datasetId)).thenReturn(this.bodhiDatasetMaps);
        when(this.bodhiDatasetMapUtil.getDatasetDescriptionByDatasetId(this.datasetId))
                .thenReturn(this.bodhiDatasetMaps);
        final List<DatasetDescriptionMap> result = this.bodhiDatasetService
                .getDatasetSchemasByDatasetId(this.datasetId);
        assertEquals(this.bodhiDatasetMaps, result);
    }

}
