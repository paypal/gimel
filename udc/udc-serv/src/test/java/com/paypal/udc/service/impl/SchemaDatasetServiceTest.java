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
import com.paypal.udc.dao.integration.common.SourceProviderRepository;
import com.paypal.udc.dao.integration.common.TagRepository;
import com.paypal.udc.dao.integration.schema.SchemaDatasetColumnMapRepository;
import com.paypal.udc.dao.integration.schema.SchemaDatasetMapRepository;
import com.paypal.udc.entity.integration.common.DatasetTagMap;
import com.paypal.udc.entity.integration.common.Tag;
import com.paypal.udc.entity.integration.schema.SchemaDatasetColumnMap;
import com.paypal.udc.entity.integration.schema.SchemaDatasetMap;
import com.paypal.udc.util.ProviderUtil;
import com.paypal.udc.util.SchemaDatasetUtil;
import com.paypal.udc.util.StorageSystemUtil;
import com.paypal.udc.util.TagUtil;


@RunWith(SpringRunner.class)
public class SchemaDatasetServiceTest {

    @InjectMocks
    private SchemaDatasetService schemaDatasetService;

    @Mock
    SchemaDatasetMapRepository sdmr;

    @Mock
    SchemaDatasetColumnMapRepository sdcmr;

    @MockBean
    SchemaDatasetUtil schemaDatasetUtil;

    @Mock
    TagRepository tagRepository;

    @MockBean
    StorageSystemUtil systemUtil;

    @Mock
    SourceProviderRepository providerRepository;

    @Mock
    DatasetRepository datasetRepository;

    @MockBean
    TagUtil tagUtil;

    @MockBean
    ProviderUtil providerUtil;

    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss");

    private long datasetId;
    private long storageSystemId;
    private long schemaDatasetMapId;
    private long providerId;
    private long tagId;
    private SchemaDatasetMap schema;
    private List<SchemaDatasetMap> schemas;
    private SchemaDatasetColumnMap columnSchema;
    private String objectType, objectName, containerName, objectComment, tagName, providerName;
    private List<SchemaDatasetColumnMap> columnSchemas;
    private Tag tag;
    private List<Tag> tags;
    private List<Long> tagIds;
    private DatasetTagMap datasetTag;
    private List<DatasetTagMap> datasetTagMap;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        this.datasetId = 1L;
        this.storageSystemId = 1L;
        this.schemaDatasetMapId = 1L;
        this.tagId = 1L;
        this.objectComment = "Object Comment";
        this.containerName = "Container Name";
        this.objectName = "Object Name";
        this.objectType = "TABLE";
        this.providerName = "USER";
        this.columnSchemas = new ArrayList<SchemaDatasetColumnMap>();
        this.schemas = new ArrayList<SchemaDatasetMap>();
        this.schema = new SchemaDatasetMap(this.storageSystemId, this.datasetId, this.providerId, this.objectType,
                this.objectName, this.containerName, this.objectComment, "CrUser", "CrTime", "UdpUser", "UpdTime");
        this.schema.setSchemaDatasetMapId(this.schemaDatasetMapId);
        this.columnSchema = new SchemaDatasetColumnMap(this.schemaDatasetMapId, "code", "columnName", "dataType",
                "comment", "CrUser", "CrTime", "UdpUser", "UpdTime");
        this.columnSchema.setSchemaDatasetColumnMapId(1L);
        this.columnSchemas.add(this.columnSchema);
        this.schema.setSchemaDatasetColumns(this.columnSchemas);
        this.schemas.add(this.schema);
        this.tags = new ArrayList<Tag>();
        this.tagName = "tag1";
        this.providerId = 1L;
        this.tag = new Tag(this.tagName, this.providerId, "CrUser", "CrTime", "UdpUser", "UpdTime");
        this.tag.setTagId(this.tagId);
        this.tags.add(this.tag);

        this.tagIds = new ArrayList<Long>();
        this.tagIds.add(this.tagId);

        this.datasetTag = new DatasetTagMap(this.datasetId, this.tagId, "CrUser", "CrTime", "UdpUser", "UpdTime");
        this.datasetTagMap = new ArrayList<DatasetTagMap>();
        this.datasetTagMap.add(this.datasetTag);

    }

    @Test
    public void verifyGetDatasetSchemasByDatasetId() throws Exception {

        when(this.sdmr.findByStorageDatasetId(this.datasetId)).thenReturn(this.schemas);
        when(this.schemaDatasetUtil.getSchemaForDataset(this.datasetId)).thenReturn(this.schemas);
        final List<SchemaDatasetMap> result = this.schemaDatasetService.getDatasetSchemasByDatasetId(this.datasetId);
        assertEquals(this.schemas, result);
    }

    @Test
    public void verifyGetDatasetSchemaBySystemObjectAndContainer() throws Exception {

        when(this.sdmr.findByStorageSystemIdAndObjectNameAndContainerNameAndProviderId(this.storageSystemId,
                this.objectName, this.containerName, this.providerId)).thenReturn(this.schema);
        when(this.providerUtil.getProviderByName(this.providerName)).thenReturn(this.providerId);
        when(this.schemaDatasetUtil.getColumnsForSchemaDatasetId(this.schemaDatasetMapId))
                .thenReturn(this.columnSchemas);

        final SchemaDatasetMap result = this.schemaDatasetService
                .getDatasetSchemaBySystemObjectAndContainerAndProviderName(this.storageSystemId, this.containerName,
                        this.objectName, this.providerName);
        assertEquals(this.schema, result);

        verify(this.sdmr).findByStorageSystemIdAndObjectNameAndContainerNameAndProviderId(this.storageSystemId,
                this.objectName, this.containerName, this.providerId);
    }

}
