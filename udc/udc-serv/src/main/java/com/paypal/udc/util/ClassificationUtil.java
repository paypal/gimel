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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.validation.ConstraintViolationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import com.google.gson.Gson;
import com.paypal.udc.dao.classification.ClassificationAttributeRepository;
import com.paypal.udc.dao.classification.ClassificationMapRepository;
import com.paypal.udc.dao.classification.ClassificationRepository;
import com.paypal.udc.dao.dataset.DatasetChangeLogRepository;
import com.paypal.udc.dao.dataset.DatasetRepository;
import com.paypal.udc.dao.integration.common.DatasetTagMapRepository;
import com.paypal.udc.dao.integration.common.TagRepository;
import com.paypal.udc.dao.zone.ZoneRepository;
import com.paypal.udc.entity.classification.Classification;
import com.paypal.udc.entity.classification.ClassificationAttribute;
import com.paypal.udc.entity.classification.ClassificationMap;
import com.paypal.udc.entity.dataset.Dataset;
import com.paypal.udc.entity.dataset.DatasetChangeLog;
import com.paypal.udc.entity.integration.common.DatasetTagMap;
import com.paypal.udc.entity.integration.common.Tag;
import com.paypal.udc.entity.storagesystem.StorageSystem;
import com.paypal.udc.entity.zone.Zone;
import com.paypal.udc.exception.ValidationError;
import com.paypal.udc.util.enumeration.TimelineEnumeration;


@Component
public class ClassificationUtil {

    final static Logger logger = LoggerFactory.getLogger(ClassificationUtil.class);
    final Gson gson = new Gson();

    @Autowired
    private DatasetChangeLogRepository changeLogRepository;

    @Autowired
    private ClassificationRepository cr;

    @Autowired
    private ClassificationMapRepository cmr;

    @Autowired
    private ClassificationAttributeRepository car;

    @Autowired
    private ProviderUtil providerUtil;

    @Autowired
    private ZoneRepository zoneRepository;

    @Autowired
    StorageSystemUtil systemUtil;

    @Autowired
    TagRepository tagRepository;

    @Autowired
    DatasetTagMapRepository datasetTagMapRepository;

    @Autowired
    TagUtil tagUtil;

    @Autowired
    DatasetRepository datasetRepository;

    @Autowired
    DatasetUtil datasetUtil;

    public Classification addClassification(
            final Classification classification, final SimpleDateFormat sdf)
            throws ValidationError {
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final String time = sdf.format(timestamp);
        final String createdUser = classification.getCreatedUser();
        final String providerName = classification.getProviderName();
        try {
            classification.setDataSource(
                    classification.getDataSource() == null ? "" : classification.getDataSource());
            classification.setDataSourceType(
                    classification.getDataSourceType() == null ? "" : classification.getDataSourceType());
            classification.setContainerName(
                    classification.getContainerName() == null ? "" : classification.getContainerName());
            final long providerId = this.providerUtil.getProviderByName(providerName);
            classification.setProviderId(providerId);
            classification.setUpdatedUser(createdUser);
            classification.setCreatedTimestamp(time);
            classification.setUpdatedTimestamp(time);
            final Classification insertedClassification = this.cr.save(classification);
            try {
                final Map<String, String> currentKeyValue = new HashMap<String, String>();
                currentKeyValue.put("columnName", classification.getColumnName());
                currentKeyValue.put("classificationId", Long.toString(classification.getClassificationId()));
                currentKeyValue.put("classificationComment", classification.getClassificationComment());
                currentKeyValue.put("username", classification.getCreatedUser());
                final String curr = this.gson.toJson(currentKeyValue);
                final List<Long> datasetsIds = classification.getDatasetIds();
                datasetsIds.forEach((datasetId) -> {
                    final DatasetChangeLog dcl = new DatasetChangeLog(datasetId, "C",
                            TimelineEnumeration.CLASSIFICATION.getFlag(), "{}", curr, time);
                    this.changeLogRepository.save(dcl);
                });
            }
            catch (final DataIntegrityViolationException e) {
                final ValidationError v = new ValidationError();
                v.setErrorCode(HttpStatus.CONFLICT);
                v.setErrorDescription(e.getMessage());
                throw v;
            }

            return insertedClassification;
        }
        catch (final DataIntegrityViolationException e) {
            final ValidationError verror = new ValidationError();
            verror.setErrorCode(HttpStatus.BAD_REQUEST);
            verror.setErrorDescription(
                    "Duplicate Table Name: " + classification.getObjectName() + " and Column Name: "
                            + classification.getColumnName());
            throw verror;

        }
        catch (final ConstraintViolationException e) {
            final ValidationError verror = new ValidationError();
            verror.setErrorCode(HttpStatus.BAD_REQUEST);
            verror.setErrorDescription("Table Name cannot be empty");
            throw verror;
        }
    }

    public Classification editClassification(
            final Classification classification, final SimpleDateFormat sdf)
            throws ValidationError {
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final String time = sdf.format(timestamp);

        final Classification tempClassification = this.cr
                .findById(classification.getDatasetClassificationId()).orElse(null);

        if (tempClassification == null) {
            final ValidationError verror = new ValidationError();
            verror.setErrorCode(HttpStatus.BAD_REQUEST);
            verror.setErrorDescription("Invalid DatasetClassificationId");
            throw verror;
        }

        final Map<String, String> previousKeyValue = new HashMap<String, String>();
        previousKeyValue.put("columnName", tempClassification.getColumnName());
        previousKeyValue.put("classificationId", Long.toString(tempClassification.getClassificationId()));
        previousKeyValue.put("classificationComment", tempClassification.getClassificationComment());
        previousKeyValue.put("username", tempClassification.getUpdatedUser());
        final String prev = this.gson.toJson(previousKeyValue);

        tempClassification.setClassificationId(classification.getClassificationId());
        tempClassification.setClassificationComment(classification.getClassificationComment());
        tempClassification.setDatasetIds(classification.getDatasetIds());
        tempClassification.setUpdatedUser(classification.getUpdatedUser());
        tempClassification.setUpdatedTimestamp(time);
        this.cr.save(tempClassification);

        try {
            final Map<String, String> currentKeyValue = new HashMap<String, String>();
            currentKeyValue.put("columnName", classification.getColumnName());
            currentKeyValue.put("classificationId", Long.toString(classification.getClassificationId()));
            currentKeyValue.put("classificationComment", classification.getClassificationComment());
            currentKeyValue.put("username", classification.getUpdatedUser());
            final String curr = this.gson.toJson(currentKeyValue);
            final List<Long> datasetsIds = classification.getDatasetIds();
            datasetsIds.forEach((datasetId) -> {
                final DatasetChangeLog dcl = new DatasetChangeLog(datasetId, "M",
                        TimelineEnumeration.CLASSIFICATION.getFlag(), prev, curr, time);
                this.changeLogRepository.save(dcl);
            });
        }
        catch (final DataIntegrityViolationException e) {
            final ValidationError v = new ValidationError();
            v.setErrorCode(HttpStatus.CONFLICT);
            v.setErrorDescription(e.getMessage());
            throw v;
        }

        return tempClassification;

    }

    public void deleteClassificationMap(final long datasetClassificationId, final long storageDataSetId,
            final long systemId, final long zoneId, final long entityId) {
        final ClassificationMap classificationMap = this.cmr
                .findByDatasetClassificationIdAndStorageDataSetIdAndStorageSystemIdAndZoneIdAndEntityId(
                        datasetClassificationId, storageDataSetId, systemId, zoneId, entityId);
        this.cmr.delete(classificationMap);
    }

    public List<ClassificationMap> editClassificationMap(final Classification classification,
            final SimpleDateFormat sdf)
            throws ValidationError {

        final Long currDatasetClassificationId = classification.getDatasetClassificationId();

        List<ClassificationMap> outputs = new ArrayList<ClassificationMap>();

        // New dataset IDs selected by user
        final List<Long> newDatasetsIds = classification.getDatasetIds();

        // All fetched rows for the currDatasetClassificationId
        final List<ClassificationMap> metastoreEntries = this.cmr
                .findByDatasetClassificationId(currDatasetClassificationId);

        // List of existing dataset IDs
        final List<Long> metastoreDatasetsIds = new ArrayList<Long>();
        final Map<Long, String> datasetToStoreMap = new HashMap<Long, String>();
        for (final ClassificationMap entry : metastoreEntries) {
            metastoreDatasetsIds.add(entry.getStorageDataSetId());
            datasetToStoreMap.put(entry.getStorageDataSetId(),
                    entry.getZoneId() + "," + entry.getEntityId() + "," + entry.getStorageSystemId());
        }

        // New dataset IDs to be inserted
        final List<Long> toBeInsertedDatasetIds = new ArrayList<Long>(newDatasetsIds);
        toBeInsertedDatasetIds.removeAll(metastoreDatasetsIds);

        // Old dataset IDs to be deleted
        final List<Long> toBeDeletedDatasetIds = new ArrayList<Long>(metastoreDatasetsIds);
        toBeDeletedDatasetIds.removeAll(newDatasetsIds);

        // Deleting old dataset IDs
        toBeDeletedDatasetIds
                .forEach(datasetId -> this.deleteClassificationMap(currDatasetClassificationId, datasetId,
                        Long.parseLong(datasetToStoreMap.get(datasetId).split(",")[2]),
                        Long.parseLong(datasetToStoreMap.get(datasetId).split(",")[0]),
                        Long.parseLong(datasetToStoreMap.get(datasetId).split(",")[1])));

        classification.setDatasetIds(toBeInsertedDatasetIds);

        // Adding new dataset IDs
        outputs = this.addClassificationMap(classification, sdf);

        return outputs;

    }

    public List<ClassificationMap> addClassificationMap(final Classification classification,
            final SimpleDateFormat sdf)
            throws ValidationError {

        final List<ClassificationMap> outputs = new ArrayList<ClassificationMap>();

        final List<Long> datasetsIds = classification.getDatasetIds();
        // validating dataset IDs
        this.datasetUtil.validateDataset(datasetsIds);
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final String time = sdf.format(timestamp);
        final String createdUser = classification.getCreatedUser();
        final String isActiveYN = "Y";

        final Map<Long, StorageSystem> systemMap = this.systemUtil.getStorageSystems();
        try {
            for (final Long dataSetId : datasetsIds) {

                final Dataset dataset = this.datasetRepository.findById(dataSetId).get();
                final long systemId = dataset.getStorageSystemId();
                final long entityId = systemMap.get(systemId).getEntityId();
                final long zoneId = systemMap.get(systemId).getZoneId();
                final ClassificationMap classificationMap = new ClassificationMap(
                        classification.getDatasetClassificationId(),
                        dataSetId, zoneId, entityId, systemId, isActiveYN, createdUser,
                        time, createdUser, time);

                outputs.add(this.cmr.save(classificationMap));
            }
            return outputs;
        }
        catch (final DataIntegrityViolationException e) {
            final ValidationError verror = new ValidationError();
            verror.setErrorCode(HttpStatus.BAD_REQUEST);
            verror.setErrorDescription("Duplicated Object Name and ColumnName");
            throw verror;

        }
        catch (final ConstraintViolationException e) {
            final ValidationError verror = new ValidationError();
            verror.setErrorCode(HttpStatus.BAD_REQUEST);
            verror.setErrorDescription("Object Name cannot be empty");
            throw verror;
        }

    }
}
