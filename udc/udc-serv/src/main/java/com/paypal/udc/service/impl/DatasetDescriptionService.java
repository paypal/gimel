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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import javax.validation.ConstraintViolationException;
import org.apache.commons.lang.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import com.google.gson.Gson;
import com.paypal.udc.dao.dataset.DatasetChangeLogRepository;
import com.paypal.udc.dao.dataset.DatasetRepository;
import com.paypal.udc.dao.integration.description.DatasetColumnDescriptionMapRepository;
import com.paypal.udc.dao.integration.description.DatasetDescriptionMapRepository;
import com.paypal.udc.entity.dataset.Dataset;
import com.paypal.udc.entity.dataset.DatasetChangeLog;
import com.paypal.udc.entity.integration.description.DatasetColumnDescriptionMap;
import com.paypal.udc.entity.integration.description.DatasetDescriptionMap;
import com.paypal.udc.entity.storagesystem.StorageSystem;
import com.paypal.udc.exception.ValidationError;
import com.paypal.udc.service.IDatasetDescriptionService;
import com.paypal.udc.util.DatasetDescriptionUtil;
import com.paypal.udc.util.DatasetUtil;
import com.paypal.udc.util.ProviderUtil;
import com.paypal.udc.util.StorageSystemUtil;
import com.paypal.udc.util.UserUtil;
import com.paypal.udc.util.enumeration.TimelineEnumeration;


@Service
@Transactional(propagation = Propagation.SUPPORTS, readOnly = true)
public class DatasetDescriptionService implements IDatasetDescriptionService {

    final static Logger logger = LoggerFactory.getLogger(DatasetDescriptionService.class);
    @Autowired
    DatasetDescriptionMapRepository bdmr;

    @Autowired
    DatasetColumnDescriptionMapRepository bdcmr;

    @Autowired
    UserUtil userUtil;

    @Autowired
    DatasetDescriptionUtil datasetDescriptionUtil;

    @Autowired
    StorageSystemUtil systemUtil;

    @Autowired
    DatasetRepository datasetRepository;

    @Autowired
    private DatasetChangeLogRepository changeLogRepository;

    @Autowired
    private DatasetDescriptionMapRepository datasetDescriptionMapRepository;

    @Autowired
    ProviderUtil providerUtil;

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

    @Override
    public List<DatasetDescriptionMap> getDatasetSchemasByDatasetId(final long datasetId) {
        return this.datasetDescriptionUtil.getDatasetDescriptionByDatasetId(datasetId);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED, readOnly = false, rollbackFor = { ValidationError.class,
            ConstraintViolationException.class, DataIntegrityViolationException.class,
            TransactionSystemException.class, IOException.class, InterruptedException.class, ExecutionException.class })
    public DatasetDescriptionMap addDatasetDescriptionMap(final DatasetDescriptionMap datasetDescriptionMapInput)
            throws ValidationError, IOException, InterruptedException, ExecutionException {
        final Gson gson = new Gson();
        final String containerName = datasetDescriptionMapInput.getContainerName();
        final String objectName = datasetDescriptionMapInput.getObjectName();
        final String storageSystemName = datasetDescriptionMapInput.getStorageSystemName();
        final String createdUser = datasetDescriptionMapInput.getCreatedUser();
        final String objectComment = datasetDescriptionMapInput.getObjectComment();
        long datasetId = datasetDescriptionMapInput.getStorageDatasetId();
        final String providerName = datasetDescriptionMapInput.getProviderName();

        // set time
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final String time = sdf.format(timestamp);

        // get provider Id
        final long providerId = this.providerUtil.getProviderByName(providerName);
        // get storage system name
        final StorageSystem storageSystem = this.systemUtil.getStorageSystem(storageSystemName);
        final long storageSystemId = storageSystem.getStorageSystemId();

        // get dataset by name
        if (datasetId == 0) {
            final String datasetName = storageSystemName + "." + containerName + "." + objectName;
            final Dataset dataset = this.datasetRepository.findByStorageDataSetName(datasetName);
            datasetId = dataset == null ? 0 : dataset.getStorageDataSetId();
        }

        // insert into pc_dataset_schema_map table and pc_dataset_column_schema_map table
        final DatasetDescriptionMap datasetDescriptionMap;
        try {
            datasetDescriptionMap = this.bdmr
                    .save(new DatasetDescriptionMap(storageSystemId, providerId, datasetId, objectName, containerName,
                            objectComment, createdUser, time, createdUser, time));
        }
        catch (final DataIntegrityViolationException e) {
            final ValidationError v = new ValidationError();
            v.setErrorCode(HttpStatus.CONFLICT);
            v.setErrorDescription("Duplicated object,container,store,providerId");
            throw v;
        }

        if (datasetId != 0) {
            try {
                final Map<String, String> currentKeyValue = new HashMap<String, String>();
                currentKeyValue.put("value", datasetDescriptionMap.getObjectComment());
                currentKeyValue.put("username", datasetDescriptionMap.getCreatedUser());
                final String curr = gson.toJson(currentKeyValue);
                final DatasetChangeLog dcl = new DatasetChangeLog(datasetDescriptionMap.getStorageDatasetId(), "C",
                        TimelineEnumeration.OBJECT_DESC.getFlag(), "{}", curr, time);
                this.changeLogRepository.save(dcl);
            }
            catch (final DataIntegrityViolationException e) {
                final ValidationError v = new ValidationError();
                v.setErrorCode(HttpStatus.CONFLICT);
                v.setErrorDescription(e.getMessage());
                throw v;
            }
        }

        if (datasetId == 0) {
            datasetId = datasetDescriptionMap.getBodhiDatasetMapId() + System.currentTimeMillis() / 1000;
            this.bdmr.save(
                    new DatasetDescriptionMap(datasetDescriptionMap.getBodhiDatasetMapId(), storageSystemId, providerId,
                            datasetId, objectName, containerName, objectComment, createdUser, time, createdUser, time));
        }

        // insert into pc_dataset_column_schema_map table
        try {
            final List<DatasetColumnDescriptionMap> columns = datasetDescriptionMapInput.getColumns();
            if (columns != null && columns.size() > 0) {
                final List<DatasetColumnDescriptionMap> modifiedColumns = new ArrayList<DatasetColumnDescriptionMap>();
                columns.forEach(column -> {
                    final DatasetColumnDescriptionMap schemaDatasetColumnMap = new DatasetColumnDescriptionMap(
                            datasetDescriptionMap.getBodhiDatasetMapId(), column.getColumnName(),
                            column.getColumnComment(), createdUser, time, createdUser, time);
                    modifiedColumns.add(schemaDatasetColumnMap);
                });
                this.bdcmr.saveAll(modifiedColumns);
            }
        }
        catch (final DataIntegrityViolationException e) {
            final ValidationError v = new ValidationError();
            v.setErrorCode(HttpStatus.CONFLICT);
            v.setErrorDescription("Duplicated bodhi_dataset_map_id,column_name");
            throw v;
        }

        if (this.isEsWriteEnabled.equals("true")) {
            final Dataset tempDataset = this.datasetRepository.findById(datasetId).orElse(null);
            if (tempDataset != null) {
                this.datasetUtil.upsertDataset(this.esDatasetIndex, this.esType, tempDataset, this.esTemplate);
            }
        }
        return datasetDescriptionMap;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED, readOnly = false, rollbackFor = { ValidationError.class,
            ConstraintViolationException.class, DataIntegrityViolationException.class, IndexOutOfBoundsException.class,
            TransactionSystemException.class, IOException.class, InterruptedException.class, ExecutionException.class })
    public DatasetDescriptionMap updateDatasetDescriptionMap(final DatasetDescriptionMap datasetDescriptionMap)
            throws ValidationError, IOException, InterruptedException, ExecutionException {
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final String time = sdf.format(timestamp);
        final ValidationError v = new ValidationError();
        DatasetDescriptionMap retrievedDatasetDescMap = null;

        try {

            retrievedDatasetDescMap = this.datasetDescriptionUtil
                    .validateDatasetDescriptionMapId(datasetDescriptionMap.getBodhiDatasetMapId());
            final String updatedUser = datasetDescriptionMap.getUpdatedUser();
            retrievedDatasetDescMap.setStorageDatasetId(datasetDescriptionMap.getStorageDatasetId());
            retrievedDatasetDescMap.setObjectComment(datasetDescriptionMap.getObjectComment());
            retrievedDatasetDescMap.setUpdatedUser(updatedUser);
            retrievedDatasetDescMap.setUpdatedTimestamp(time);
            retrievedDatasetDescMap = this.bdmr.save(retrievedDatasetDescMap);

        }
        catch (final TransactionSystemException e) {
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("Dataset description is empty");
            throw v;
        }

        try {
            final List<DatasetChangeLog> changeLogList = this.changeLogRepository
                    .findByStorageDataSetIdAndChangeColumnType(retrievedDatasetDescMap.getStorageDatasetId(),
                            "OBJECT_DESC");
            final String curr = "{\"value\": \"" + retrievedDatasetDescMap.getObjectComment() +
                    "\", \"username\": \"" + retrievedDatasetDescMap.getUpdatedUser() + "\"}";
            if (!changeLogList.isEmpty()) {
                final DatasetChangeLog tempChangeLogDataset = changeLogList.get(changeLogList.size() - 1);
                final DatasetChangeLog dcl = new DatasetChangeLog(retrievedDatasetDescMap.getStorageDatasetId(), "M",
                        TimelineEnumeration.OBJECT_DESC.getFlag(), tempChangeLogDataset.getColumnCurrValInString(),
                        curr,
                        time);
                this.changeLogRepository.save(dcl);
            }
            else {
                final String comment = retrievedDatasetDescMap.getObjectComment();
                final String prevUser = retrievedDatasetDescMap.getUpdatedUser();
                final String prevComment = "{\"value\": \"" + comment +
                        "\", \"username\": \"" + prevUser + "\"}";
                final DatasetChangeLog dcl = new DatasetChangeLog(retrievedDatasetDescMap.getStorageDatasetId(), "M",
                        TimelineEnumeration.OBJECT_DESC.getFlag(), prevComment, curr,
                        time);
                this.changeLogRepository.save(dcl);
            }
        }
        catch (final DataIntegrityViolationException e) {
            v.setErrorCode(HttpStatus.CONFLICT);
            v.setErrorDescription("Duplicated change_log_id and dataset ID");
            throw v;
        }

        if (this.isEsWriteEnabled.equals("true")) {
            final Dataset tempDataset = this.datasetRepository.findById(retrievedDatasetDescMap.getStorageDatasetId())
                    .orElse(null);
            if (tempDataset != null) {
                this.datasetUtil.upsertDataset(this.esDatasetIndex, this.esType, tempDataset, this.esTemplate);
            }
        }
        return retrievedDatasetDescMap;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED, readOnly = false, rollbackFor = { ValidationError.class,
            ConstraintViolationException.class, DataIntegrityViolationException.class,
            TransactionSystemException.class, IOException.class, InterruptedException.class, ExecutionException.class })
    public void updateDatasetColumnDescriptionMap(final DatasetColumnDescriptionMap datasetColumnDescriptionMap)
            throws ValidationError, IOException, InterruptedException, ExecutionException {

        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final Gson gson = new Gson();
        final String time = sdf.format(timestamp);
        DatasetColumnDescriptionMap retrievedDatasetColumnDescMap = this.bdcmr
                .findById(datasetColumnDescriptionMap.getBodhiDatasetColumnMapId())
                .orElse(null);

        final String updatedUser = datasetColumnDescriptionMap.getUpdatedUser();
        if (retrievedDatasetColumnDescMap != null) {
            final Map<String, String> previousKeyValue = new HashMap<String, String>();
            previousKeyValue.put("columnName", retrievedDatasetColumnDescMap.getColumnName().replaceAll("\"", ""));
            previousKeyValue.put("columnDesc", StringEscapeUtils
                    .escapeJava(retrievedDatasetColumnDescMap.getColumnComment().replaceAll("[\\n|\\t|\\s|\\r]",
                            "")));
            previousKeyValue.put("username", retrievedDatasetColumnDescMap.getUpdatedUser());
            final String prev = gson.toJson(previousKeyValue);

            // this.userUtil.validateUser(updatedUser);
            this.datasetDescriptionUtil
                    .validateDatasetDescriptionMapId(datasetColumnDescriptionMap.getBodhiDatasetMapId());
            retrievedDatasetColumnDescMap.setColumnComment(datasetColumnDescriptionMap.getColumnComment());
            retrievedDatasetColumnDescMap.setUpdatedUser(updatedUser);
            retrievedDatasetColumnDescMap.setUpdatedTimestamp(time);
            retrievedDatasetColumnDescMap = this.bdcmr.save(retrievedDatasetColumnDescMap);
            final DatasetDescriptionMap description = this.bdmr
                    .findById(retrievedDatasetColumnDescMap.getBodhiDatasetMapId()).orElse(null);

            final Dataset tempDataset = this.datasetRepository.findById(description.getStorageDatasetId())
                    .orElse(null);

            final Map<String, String> currentKeyValue = new HashMap<String, String>();
            currentKeyValue.put("columnName", retrievedDatasetColumnDescMap.getColumnName().replaceAll("\"", ""));
            currentKeyValue.put("columnDesc", StringEscapeUtils
                    .escapeJava(retrievedDatasetColumnDescMap.getColumnComment().replaceAll("[\\n|\\t|\\s|\\r]",
                            "")));
            currentKeyValue.put("username", retrievedDatasetColumnDescMap.getUpdatedUser());
            final String curr = gson.toJson(currentKeyValue);
            final DatasetChangeLog dcl = new DatasetChangeLog(tempDataset.getStorageDataSetId(),
                    "M",
                    TimelineEnumeration.COLUMN_DESC.getFlag(), prev,
                    curr,
                    time);
            this.changeLogRepository.save(dcl);
        }
        else {
            final ValidationError v = new ValidationError();
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("BodhiDatasetMapId is invalid");
            throw v;
        }

        if (this.isEsWriteEnabled.equals("true")) {
            final DatasetDescriptionMap bodhiDatasetMap = this.bdmr
                    .findById(retrievedDatasetColumnDescMap.getBodhiDatasetMapId()).orElse(null);
            final Dataset tempDataset = this.datasetRepository.findById(bodhiDatasetMap.getStorageDatasetId())
                    .orElse(null);
            if (tempDataset != null) {
                this.datasetUtil.upsertDataset(this.esDatasetIndex, this.esType, tempDataset, this.esTemplate);
            }
        }

    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED, readOnly = false, rollbackFor = { ValidationError.class,
            ConstraintViolationException.class, DataIntegrityViolationException.class,
            TransactionSystemException.class, IOException.class, InterruptedException.class, ExecutionException.class })
    public DatasetColumnDescriptionMap addDatasetColumnDecriptionMap(final DatasetColumnDescriptionMap dcdm)
            throws ValidationError, IOException, InterruptedException, ExecutionException {
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final String time = sdf.format(timestamp);
        DatasetColumnDescriptionMap datasetColumnDescriptionMap;
        try {
            datasetColumnDescriptionMap = this.bdcmr
                    .save(new DatasetColumnDescriptionMap(dcdm.getBodhiDatasetMapId(), dcdm.getColumnName(),
                            dcdm.getColumnComment(), dcdm.getCreatedUser(), time, dcdm.getCreatedUser(), time));
        }
        catch (final DataIntegrityViolationException e) {
            final ValidationError v = new ValidationError();
            v.setErrorCode(HttpStatus.CONFLICT);
            v.setErrorDescription("Duplicated bodhi_dataset_map_id,column_name");
            throw v;
        }
        this.datasetDescriptionUtil.insertChangeLogForUpdateDatasetColumnDescription(datasetColumnDescriptionMap,
                "{}", "C");
        if (this.isEsWriteEnabled.equals("true")) {
            final DatasetDescriptionMap description = this.bdmr
                    .findById(datasetColumnDescriptionMap.getBodhiDatasetMapId()).orElse(null);
            final Dataset tempDataset = this.datasetRepository.findById(description.getStorageDatasetId())
                    .orElse(null);
            if (tempDataset != null) {
                this.datasetUtil.upsertDataset(this.esDatasetIndex, this.esType, tempDataset, this.esTemplate);
            }
        }
        return datasetColumnDescriptionMap;
    }

    @Override
    public DatasetColumnDescriptionMap getDatasetColumnDescriptionMapByIdAndColumnName(final long id,
            final String columnName) {
        final DatasetColumnDescriptionMap datasetColumn = this.bdcmr.findByBodhiDatasetMapIdAndColumnName(id,
                columnName);
        if (datasetColumn != null) {
            return datasetColumn;
        }
        else {
            return new DatasetColumnDescriptionMap();
        }
    }

    @Override
    public DatasetDescriptionMap getDatasetDescriptionMapBySystemContainerAndObjectAndProvider(final long id,
            final String containerName, final String objectName, final String providerName) {

        try {
            final long providerId = this.providerUtil.getProviderByName(providerName);
            final DatasetDescriptionMap datasetDescriptionMap = this.bdmr
                    .findByStorageSystemIdAndContainerNameAndObjectNameAndProviderId(id, containerName,
                            objectName, providerId);
            if (datasetDescriptionMap != null) {
                final List<DatasetColumnDescriptionMap> columnDescriptions = this.bdcmr
                        .findByBodhiDatasetMapId(datasetDescriptionMap.getBodhiDatasetMapId());
                datasetDescriptionMap.setColumns(columnDescriptions);
                return datasetDescriptionMap;
            }
            else {
                return new DatasetDescriptionMap();
            }

        }
        catch (final ValidationError e) {
            return new DatasetDescriptionMap();
        }

    }

}
