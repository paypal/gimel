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

package com.paypal.udc.util;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import com.google.gson.Gson;
import com.paypal.udc.dao.dataset.DatasetChangeLogRepository;
import com.paypal.udc.dao.dataset.DatasetRepository;
import com.paypal.udc.dao.integration.description.DatasetColumnDescriptionMapRepository;
import com.paypal.udc.dao.integration.description.DatasetDescriptionMapRepository;
import com.paypal.udc.entity.dataset.Dataset;
import com.paypal.udc.entity.dataset.DatasetChangeLog;
import com.paypal.udc.entity.dataset.DatasetWithAttributes;
import com.paypal.udc.entity.integration.common.DatasetColumnDescription;
import com.paypal.udc.entity.integration.common.DatasetDescription;
import com.paypal.udc.entity.integration.common.SourceProvider;
import com.paypal.udc.entity.integration.common.Tag;
import com.paypal.udc.entity.integration.description.DatasetColumnDescriptionMap;
import com.paypal.udc.entity.integration.description.DatasetDescriptionMap;
import com.paypal.udc.exception.ValidationError;
import com.paypal.udc.util.enumeration.TimelineEnumeration;


@Component
public class DatasetDescriptionUtil {

    final static Logger logger = LoggerFactory.getLogger(DatasetDescriptionUtil.class);

    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss");
    @Autowired
    private DatasetDescriptionMapRepository bdmr;

    @Autowired
    private DatasetColumnDescriptionMapRepository bdcmr;

    @Autowired
    TagUtil tagUtil;

    @Autowired
    ProviderUtil providerUtil;

    @Autowired
    DatasetRepository datasetRepository;

    @Autowired
    private DatasetChangeLogRepository changeLogRepository;

    public List<DatasetColumnDescriptionMap> getColumnsDescriptionForDatasetId(final Long datasetDescriptionMapId) {
        final List<DatasetColumnDescriptionMap> columns = this.bdcmr
                .findByBodhiDatasetMapId(datasetDescriptionMapId);
        return columns;
    }

    public DatasetDescriptionMap getDatasetDescriptionMapBySystemContainerAndObject(final long systemId,
            final String containerName, final String objectName) {
        final List<DatasetDescriptionMap> datasetDescriptionMaps = this.bdmr
                .findByStorageSystemIdAndContainerNameAndObjectName(systemId,
                        containerName, objectName);

        if (datasetDescriptionMaps != null && datasetDescriptionMaps.size() > 0) {
            final DatasetDescriptionMap datasetDescriptionMap = datasetDescriptionMaps.stream()
                    .max(Comparator.comparing(DatasetDescriptionMap::getUpdatedTimestamp))
                    .get();
            if (datasetDescriptionMap != null) {
                final List<DatasetColumnDescriptionMap> columnMap = this
                        .getColumnsDescriptionForDatasetId(datasetDescriptionMap.getBodhiDatasetMapId());
                datasetDescriptionMap.setColumns(columnMap);
                return datasetDescriptionMap;
            }
        }
        return new DatasetDescriptionMap();
    }

    public List<DatasetDescriptionMap> getDatasetDescriptionByDatasetId(final long datasetId) {
        final List<DatasetDescriptionMap> datasetDescriptionMap = this.bdmr.findByStorageDatasetId(datasetId);

        datasetDescriptionMap.forEach(description -> {
            final List<DatasetColumnDescriptionMap> columnMap = this
                    .getColumnsDescriptionForDatasetId(description.getBodhiDatasetMapId());
            description.setColumns(columnMap);
        });

        return datasetDescriptionMap;
    }

    public DatasetDescriptionMap validateDatasetDescriptionMapId(final long datasetDescriptionMapId)
            throws ValidationError {
        final DatasetDescriptionMap datasetDescriptionMap = this.bdmr.findById(datasetDescriptionMapId)
                .orElse(null);
        if (datasetDescriptionMap == null) {
            final ValidationError verror = new ValidationError();
            verror.setErrorCode(HttpStatus.BAD_REQUEST);
            verror.setErrorDescription("Invalid Bodhi Dataset Map Id");
            throw verror;
        }
        return datasetDescriptionMap;
    }

    public void setDatasetSchemaAndTagsForDataset(final DatasetWithAttributes dataset,
            final long storageSystemId, final String containerName, final String objectName) throws ValidationError {

        final DatasetDescriptionMap datasetDescriptionMap = this.getDatasetDescriptionMapBySystemContainerAndObject(
                storageSystemId, containerName, objectName);
        if (datasetDescriptionMap.getBodhiDatasetMapId() != 0) {
            final List<DatasetColumnDescriptionMap> columnDescriptions = datasetDescriptionMap.getColumns();
            final List<DatasetColumnDescription> datasetColumnDescriptions = new ArrayList<DatasetColumnDescription>();

            if (columnDescriptions != null && columnDescriptions.size() > 0) {
                columnDescriptions.forEach(columnDescription -> {
                    final DatasetColumnDescription datasetColumnDescription = new DatasetColumnDescription(
                            columnDescription.getColumnName(), columnDescription.getColumnComment(),
                            columnDescription.getCreatedTimestamp(), columnDescription.getUpdatedTimestamp());
                    datasetColumnDescriptions.add(datasetColumnDescription);
                });
            }
            final SourceProvider provider = this.providerUtil.validateProvider(datasetDescriptionMap.getProviderId());
            final String providerName = provider.getSourceProviderName().equals("USER")
                    ? datasetDescriptionMap.getCreatedUser() : provider.getSourceProviderName();
            final DatasetDescription datasetDescription = new DatasetDescription(
                    dataset.getStorageDataSetName(), datasetDescriptionMap.getObjectComment(),
                    datasetColumnDescriptions, providerName);
            dataset.setSchemaDescription(datasetDescription);
            final List<Tag> tags = this.tagUtil.getTagsByDatasetId(datasetDescriptionMap.getStorageDatasetId());
            dataset.setTags(tags);
        }
        else {
            final List<Tag> tags = this.tagUtil.getTagsByDatasetId(dataset.getStorageDataSetId());
            dataset.setTags(tags);
        }
    }

    public void insertChangeLogForUpdateDatasetColumnDescription(
            final DatasetColumnDescriptionMap retrievedDatasetColumnDescMap, final String prev,
            final String changeType)
            throws ValidationError {
        try {
            final DatasetDescriptionMap description = this.bdmr
                    .findById(retrievedDatasetColumnDescMap.getBodhiDatasetMapId()).orElse(null);
            final Dataset tempDataset = this.datasetRepository.findById(description.getStorageDatasetId())
                    .orElse(null);

            final Gson gson = new Gson();

            final Map<String, String> currentKeyValue = new HashMap<String, String>();
            currentKeyValue.put("columnName", retrievedDatasetColumnDescMap.getColumnName().replaceAll("\"", ""));
            currentKeyValue.put("columnDesc", StringEscapeUtils
                    .escapeJava(retrievedDatasetColumnDescMap.getColumnComment().replaceAll("[\\n|\\t|\\s|\\r]",
                            "")));
            currentKeyValue.put("username", retrievedDatasetColumnDescMap.getCreatedUser());
            final String curr = gson.toJson(currentKeyValue);
            final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
            final String time = sdf.format(timestamp);
            final DatasetChangeLog dcl = new DatasetChangeLog(tempDataset.getStorageDataSetId(),
                    changeType,
                    TimelineEnumeration.COLUMN_DESC.getFlag(), prev,
                    curr,
                    time);
            this.changeLogRepository.save(dcl);
        }
        catch (final DataIntegrityViolationException e) {
            final ValidationError v = new ValidationError();
            v.setErrorCode(HttpStatus.CONFLICT);
            v.setErrorDescription("Duplicated change_log_id and dataset ID");
            throw v;
        }
    }

}
