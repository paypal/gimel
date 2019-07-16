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

import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import javax.validation.ConstraintViolationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import com.paypal.udc.dao.dataset.DatasetRepository;
import com.paypal.udc.dao.integration.common.DatasetTagMapRepository;
import com.paypal.udc.dao.integration.common.TagRepository;
import com.paypal.udc.dao.integration.schema.SchemaDatasetColumnMapRepository;
import com.paypal.udc.dao.integration.schema.SchemaDatasetMapRepository;
import com.paypal.udc.entity.dataset.Dataset;
import com.paypal.udc.entity.integration.common.DatasetTagMap;
import com.paypal.udc.entity.integration.common.Tag;
import com.paypal.udc.entity.integration.schema.SchemaDatasetColumnMap;
import com.paypal.udc.entity.integration.schema.SchemaDatasetColumnMapInput;
import com.paypal.udc.entity.integration.schema.SchemaDatasetMap;
import com.paypal.udc.entity.integration.schema.SchemaDatasetMapInput;
import com.paypal.udc.entity.storagesystem.StorageSystem;
import com.paypal.udc.exception.ValidationError;
import com.paypal.udc.service.ISchemaDatasetService;
import com.paypal.udc.util.DatasetUtil;
import com.paypal.udc.util.ProviderUtil;
import com.paypal.udc.util.SchemaDatasetUtil;
import com.paypal.udc.util.StorageSystemUtil;
import com.paypal.udc.util.TagUtil;
import com.paypal.udc.util.UserUtil;


@Service
@Transactional(propagation = Propagation.SUPPORTS, readOnly = true)
public class SchemaDatasetService implements ISchemaDatasetService {

    @Autowired
    SchemaDatasetMapRepository sdmr;

    @Autowired
    SchemaDatasetColumnMapRepository sdcmr;

    @Autowired
    UserUtil userUtil;

    @Autowired
    DatasetTagMapRepository datasetTagRepository;

    @Autowired
    SchemaDatasetUtil schemaDatasetUtil;

    @Autowired
    TagRepository tagRepository;

    @Autowired
    StorageSystemUtil systemUtil;

    @Autowired
    ProviderUtil providerUtil;

    @Autowired
    DatasetRepository datasetRepository;

    @Autowired
    DatasetTagMapRepository datasetTagMapRepository;

    @Autowired
    TagUtil tagUtil;

    @Value("${elasticsearch.dataset.name}")
    private String esDatasetIndex;

    @Value("${elasticsearch.type.name}")
    private String esType;

    @Value("${udc.es.write.enabled}")
    private String isEsWriteEnabled;

    @Autowired
    private ElasticsearchTemplate esTemplate;

    @Autowired
    private DatasetUtil datasetUtil;

    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss");

    String systemPrefix = "Teradata";

    String schemaName = "Schema";

    @Override
    public List<SchemaDatasetMap> getDatasetSchemasByDatasetId(final long datasetId) {
        return this.schemaDatasetUtil.getSchemaForDataset(datasetId);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED, readOnly = false, rollbackFor = { ValidationError.class,
            ConstraintViolationException.class, DataIntegrityViolationException.class, IOException.class,
            InterruptedException.class, ExecutionException.class })
    public void addSchemaDatasetMap(final SchemaDatasetMapInput schemaDatasetMapInput)
            throws ValidationError, IOException, InterruptedException, ExecutionException {

        final String containerName = schemaDatasetMapInput.getSchema_name();
        final String objectType = schemaDatasetMapInput.getObject_type();
        final String objectName = schemaDatasetMapInput.getName();
        final String modelName = schemaDatasetMapInput.getModel_name().toLowerCase();
        final String createdUser = schemaDatasetMapInput.getCreatedUser() == null ? "udc_admin"
                : schemaDatasetMapInput.getCreatedUser();
        final String providerName = schemaDatasetMapInput.getProviderName() == null ? this.schemaName
                : schemaDatasetMapInput.getProviderName();

        // set time
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final String time = sdf.format(timestamp);

        // get source provider
        final long providerId = this.providerUtil.getProviderByName(providerName);

        // from db_server capture the storage system id
        final StorageSystem storageSystem = this.systemUtil
                .getStorageSystem(this.systemPrefix + "." + schemaDatasetMapInput.getDb_server());
        if (storageSystem == null) {
            final ValidationError verror = new ValidationError();
            verror.setErrorCode(HttpStatus.CONFLICT);
            verror.setErrorDescription("Invalid Storage System Name");
            throw verror;
        }
        final long storageSystemId = storageSystem.getStorageSystemId();

        // get dataset by name
        Dataset dataset;
        if (objectType.equals("TABLE")) {
            final String datasetNameAtTableLevel = storageSystem.getStorageSystemName() + "." + containerName + "."
                    + objectName;
            final String datasetNameAtViewLevel = storageSystem.getStorageSystemName() + "." + containerName + "_VIEW"
                    + "." + objectName;
            dataset = this.datasetRepository.findByStorageDataSetName(datasetNameAtTableLevel);
            if (dataset == null) {
                dataset = this.datasetRepository.findByStorageDataSetName(datasetNameAtViewLevel);
            }
        }
        else {
            final String datasetNameAtViewLevel = storageSystem.getStorageSystemName() + "." + containerName + "."
                    + objectName;
            dataset = this.datasetRepository.findByStorageDataSetName(datasetNameAtViewLevel);
        }

        long datasetId = dataset == null ? 0 : dataset.getStorageDataSetId();

        // insert into pc_dataset_schema_map table and pc_dataset_column_schema_map table
        SchemaDatasetMap schemaDatasetMap;
        try {
            schemaDatasetMap = this.sdmr
                    .save(new SchemaDatasetMap(storageSystemId, datasetId, providerId, objectType, objectName,
                            containerName, schemaDatasetMapInput.getComment(), createdUser, time, createdUser, time));
        }
        catch (final DataIntegrityViolationException e) {
            final ValidationError v = new ValidationError();
            v.setErrorCode(HttpStatus.CONFLICT);
            v.setErrorDescription("Duplicated system id, container name and object name");
            throw v;
        }

        if (datasetId == 0) {
            datasetId = schemaDatasetMap.getSchemaDatasetMapId() + System.currentTimeMillis() / 1000;
            this.sdmr.save(new SchemaDatasetMap(schemaDatasetMap.getSchemaDatasetMapId(), storageSystemId, datasetId,
                    providerId, objectType, objectName, containerName, schemaDatasetMapInput.getComment(), createdUser,
                    time, createdUser, time));
        }
        // insert into pc_dataset_column_schema_map table
        try {
            final List<SchemaDatasetColumnMapInput> columns = schemaDatasetMapInput.getColumns();
            final List<SchemaDatasetColumnMap> modifiedColumns = new ArrayList<SchemaDatasetColumnMap>();
            columns.forEach(column -> {
                final SchemaDatasetColumnMap schemaDatasetColumnMap = new SchemaDatasetColumnMap(
                        schemaDatasetMap.getSchemaDatasetMapId(), column.getCode(), column.getName(),
                        column.getData_type(),
                        column.getComment(), createdUser, time, createdUser, time);
                modifiedColumns.add(schemaDatasetColumnMap);
            });
            this.sdcmr.saveAll(modifiedColumns);
        }
        catch (final DataIntegrityViolationException e) {
            final ValidationError v = new ValidationError();
            v.setErrorCode(HttpStatus.CONFLICT);
            v.setErrorDescription("Duplicated schema_dataset_map_id,column_code,column_name,column_datatype");
            throw v;
        }

        // from model_name insert into pc_dataset_tag_map table and pc_tags
        Tag tag = this.tagRepository.findByTagNameAndProviderIdAndCreatedUser(
                schemaDatasetMapInput.getModel_name().toLowerCase(),
                providerId, createdUser);
        if (tag == null) {
            // post the tag
            tag = new Tag(modelName, providerId, createdUser, time, createdUser, time);
            tag = this.tagUtil.postTag(tag, sdf);
        }
        // post to pc_dataset_tag_map table
        try {
            DatasetTagMap datasetTagMap = new DatasetTagMap(datasetId, tag.getTagId(), createdUser, time,
                    createdUser, time);
            datasetTagMap = this.datasetTagMapRepository.save(datasetTagMap);
        }
        catch (final DataIntegrityViolationException e) {
            final ValidationError v = new ValidationError();
            v.setErrorCode(HttpStatus.CONFLICT);
            v.setErrorDescription("Duplicated tag_id and dataset_id");
            throw v;
        }

        if (this.isEsWriteEnabled.equals("true")) {
            final Dataset tempDataset = this.datasetRepository.findById(datasetId).orElse(null);
            if (tempDataset != null) {
                this.datasetUtil.upsertDataset(this.esDatasetIndex, this.esType, tempDataset, this.esTemplate);
            }
        }

    }

    @Override
    public SchemaDatasetMap getDatasetSchemaBySystemObjectAndContainerAndProviderName(final long storageSystemId,
            final String containerName, final String objectName, final String providerName) throws ValidationError {
        final long providerId = this.providerUtil.getProviderByName(providerName);
        final SchemaDatasetMap schemaDatasetMap = this.sdmr
                .findByStorageSystemIdAndObjectNameAndContainerNameAndProviderId(storageSystemId, objectName,
                        containerName, providerId);
        if (schemaDatasetMap != null) {
            final List<SchemaDatasetColumnMap> columnMap = this.schemaDatasetUtil
                    .getColumnsForSchemaDatasetId(schemaDatasetMap.getSchemaDatasetMapId());
            schemaDatasetMap.setSchemaDatasetColumns(columnMap);
            return schemaDatasetMap;
        }
        return new SchemaDatasetMap();
    }

    @Override
    public SchemaDatasetMap updateSchemaDatasetMap(final SchemaDatasetMap schemaDatasetMap)
            throws ValidationError, IOException, InterruptedException, ExecutionException {
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final String time = sdf.format(timestamp);
        SchemaDatasetMap retrievedSchemaDatasetMap = this.sdmr.findById(schemaDatasetMap.getSchemaDatasetMapId())
                .orElse(null);
        final String updatedUser = schemaDatasetMap.getUpdatedUser();
        if (retrievedSchemaDatasetMap != null) {
            // this.userUtil.validateUser(updatedUser);
            final long providerId = this.providerUtil.getProviderByName(schemaDatasetMap.getProviderName());
            retrievedSchemaDatasetMap.setStorageDatasetId(schemaDatasetMap.getStorageDatasetId());
            retrievedSchemaDatasetMap.setObjectComment(schemaDatasetMap.getObjectComment());
            retrievedSchemaDatasetMap.setProviderId(providerId);
            retrievedSchemaDatasetMap.setUpdatedUser(updatedUser);
            retrievedSchemaDatasetMap.setUpdatedTimestamp(time);
            retrievedSchemaDatasetMap = this.sdmr.save(retrievedSchemaDatasetMap);
        }
        else {
            final ValidationError v = new ValidationError();
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("SchemaDatasetMapId is invalid");
            throw v;
        }

        if (this.isEsWriteEnabled.equals("true")) {
            final Dataset tempDataset = this.datasetRepository.findById(retrievedSchemaDatasetMap.getStorageDatasetId())
                    .orElse(null);
            if (tempDataset != null) {
                this.datasetUtil.upsertDataset(this.esDatasetIndex, this.esType, tempDataset, this.esTemplate);
            }
        }

        return retrievedSchemaDatasetMap;
    }

    @Override
    public void updateSchemaDatasetColumnMap(final SchemaDatasetColumnMap schemaDatasetColumnMap)
            throws ValidationError, IOException, InterruptedException, ExecutionException {
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final String time = sdf.format(timestamp);
        SchemaDatasetColumnMap retrievedSchemaDatasetColumnMap = this.sdcmr
                .findById(schemaDatasetColumnMap.getSchemaDatasetColumnMapId()).orElse(null);
        final String updatedUser = schemaDatasetColumnMap.getUpdatedUser();
        if (retrievedSchemaDatasetColumnMap != null) {
            // this.userUtil.validateUser(updatedUser);
            this.schemaDatasetUtil.validateSchemaDatasetMapId(schemaDatasetColumnMap.getSchemaDatasetMapId());
            retrievedSchemaDatasetColumnMap.setColumnComment(schemaDatasetColumnMap.getColumnComment());
            retrievedSchemaDatasetColumnMap.setUpdatedUser(updatedUser);
            retrievedSchemaDatasetColumnMap.setUpdatedTimestamp(time);
            retrievedSchemaDatasetColumnMap = this.sdcmr.save(retrievedSchemaDatasetColumnMap);
        }
        else {
            final ValidationError v = new ValidationError();
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("SchemaDatasetMapId is invalid");
            throw v;
        }

        if (this.isEsWriteEnabled.equals("true")) {
            final SchemaDatasetMap schemaDatasetMap = this.sdmr
                    .findById(schemaDatasetColumnMap.getSchemaDatasetMapId()).orElse(null);
            final Dataset tempDataset = this.datasetRepository.findById(schemaDatasetMap.getStorageDatasetId())
                    .orElse(null);
            if (tempDataset != null) {
                this.datasetUtil.upsertDataset(this.esDatasetIndex, this.esType, tempDataset, this.esTemplate);
            }
        }

    }

    @Override
    public SchemaDatasetMap addDatasetDescriptionMapViaUDC(final SchemaDatasetMap schemaDatasetMap)
            throws ValidationError, IOException, InterruptedException, ExecutionException {

        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final String time = sdf.format(timestamp);
        SchemaDatasetMap insertedSchemaDatasetMap;
        try {
            final long providerId = this.providerUtil.getProviderByName(schemaDatasetMap.getProviderName());
            insertedSchemaDatasetMap = this.sdmr.save(new SchemaDatasetMap(schemaDatasetMap.getStorageSystemId(),
                    schemaDatasetMap.getStorageDatasetId(), providerId, schemaDatasetMap.getObjectType(),
                    schemaDatasetMap.getObjectName(), schemaDatasetMap.getContainerName(),
                    schemaDatasetMap.getObjectComment(), schemaDatasetMap.getCreatedUser(), time,
                    schemaDatasetMap.getCreatedUser(), time));
        }
        catch (final DataIntegrityViolationException e) {
            final ValidationError v = new ValidationError();
            v.setErrorCode(HttpStatus.CONFLICT);
            v.setErrorDescription("Duplicated system id, container name and object name");
            throw v;
        }

        if (this.isEsWriteEnabled.equals("true")) {
            final Dataset tempDataset = this.datasetRepository.findById(insertedSchemaDatasetMap.getStorageDatasetId())
                    .orElse(null);
            if (tempDataset != null) {
                this.datasetUtil.upsertDataset(this.esDatasetIndex, this.esType, tempDataset, this.esTemplate);
            }
        }

        return insertedSchemaDatasetMap;
    }

    @Override
    public SchemaDatasetColumnMap addDatasetColumnDescriptionMapViaUDC(
            final SchemaDatasetColumnMap schemaDatasetColumnMap)
            throws ValidationError, IOException, InterruptedException, ExecutionException {
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final String time = sdf.format(timestamp);
        SchemaDatasetColumnMap insertedSchemaDatasetColumnMap;
        try {
            insertedSchemaDatasetColumnMap = this.sdcmr
                    .save(new SchemaDatasetColumnMap(schemaDatasetColumnMap.getSchemaDatasetMapId(),
                            schemaDatasetColumnMap.getColumnCode(), schemaDatasetColumnMap.getColumnName(),
                            schemaDatasetColumnMap.getColumnDatatype(), schemaDatasetColumnMap.getColumnComment(),
                            schemaDatasetColumnMap.getCreatedUser(), time, schemaDatasetColumnMap.getCreatedUser(),
                            time));
        }
        catch (final DataIntegrityViolationException e) {
            final ValidationError v = new ValidationError();
            v.setErrorCode(HttpStatus.CONFLICT);
            v.setErrorDescription("Duplicated schema_dataset_map_id,column_code,column_name,column_datatype");
            throw v;
        }

        if (this.isEsWriteEnabled.equals("true")) {
            final SchemaDatasetMap schemaDatasetMap = this.sdmr
                    .findById(schemaDatasetColumnMap.getSchemaDatasetMapId()).orElse(null);
            final Dataset tempDataset = this.datasetRepository.findById(schemaDatasetMap.getStorageDatasetId())
                    .orElse(null);
            if (tempDataset != null) {
                this.datasetUtil.upsertDataset(this.esDatasetIndex, this.esType, tempDataset, this.esTemplate);
            }
        }
        return insertedSchemaDatasetColumnMap;
    }

    @Override
    public SchemaDatasetColumnMap getDatasetColumnBySchemaIdColumnNameAndDataType(final long id,
            final String columnName, final String columnDataType) {
        final SchemaDatasetColumnMap datasetColumn = this.sdcmr
                .findBySchemaDatasetMapIdAndColumnCodeAndColumnNameAndColumnDatatype(id, columnName, columnName,
                        columnDataType);
        if (datasetColumn != null) {
            return datasetColumn;
        }
        else {
            return new SchemaDatasetColumnMap();
        }
    }

}
