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

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import com.paypal.udc.dao.classification.ClassificationRepository;
import com.paypal.udc.dao.classification.PageableClassificationRepository;
import com.paypal.udc.dao.integration.common.SourceProviderRepository;
import com.paypal.udc.entity.classification.Classification;
import com.paypal.udc.entity.classification.ClassificationDatasetMap;
import com.paypal.udc.entity.classification.ClassificationMap;
import com.paypal.udc.entity.dataset.Dataset;
import com.paypal.udc.entity.integration.common.SourceProvider;
import com.paypal.udc.exception.ValidationError;
import com.paypal.udc.service.IClassificationDatasetService;
import com.paypal.udc.util.ClassificationUtil;
import com.paypal.udc.util.ObjectSchemaMapUtil;

@Service
@Transactional(propagation = Propagation.SUPPORTS, readOnly = true)
public class ClassificationDatasetService implements IClassificationDatasetService {

    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss");

    @Autowired
    private ClassificationRepository odcmr;

    @Autowired
    private ClassificationUtil classificationUtil;

    @Autowired
    private PageableClassificationRepository pageClassificationRepository;

    @Autowired
    private ObjectSchemaMapUtil schemaMapUtil;

    @Autowired
    private SourceProviderRepository spr;

    @Override
    public Page<Classification> getAllClassifications(final Pageable pageable) throws ValidationError {
        final List<SourceProvider> providers = new ArrayList<SourceProvider>();
        this.spr.findAll().forEach(providers::add);

        final Page<Classification> pagedClassifications = this.pageClassificationRepository.findAll(pageable);
        if (pagedClassifications != null) {
            pagedClassifications.getContent().forEach(classification -> {
                classification.setProviderName(
                        providers.stream()
                                .filter(provider -> provider.getSourceProviderId() == classification.getProviderId())
                                .findFirst().get().getSourceProviderName());
            });
        }
        return pagedClassifications;
    }

    @Override
    public Page<Classification> getClassificationByClassId(final Pageable pageable, final long classificationId) {
        final List<SourceProvider> providers = new ArrayList<SourceProvider>();
        this.spr.findAll().forEach(providers::add);

        final Page<Classification> pagedClassifications = this.pageClassificationRepository.findByClassificationId(
                pageable,
                classificationId);
        if (pagedClassifications != null) {
            pagedClassifications.getContent().forEach(classification -> {
                classification.setProviderName(
                        providers.stream()
                                .filter(provider -> provider.getSourceProviderId() == classification.getProviderId())
                                .findFirst().get().getSourceProviderName());
            });
        }
        return pagedClassifications;
    }

    @Override
    public List<Classification> getClassificationByObjectName(final String objectName) {
        final List<Classification> classifications = this.odcmr.findByObjectName(objectName);
        return classifications;
    }

    @Override
    public List<Classification> getClassificationByColumnName(final String columnName) {
        final List<Classification> classifications = this.odcmr.findByColumnName(columnName);
        return classifications;
    }

    @Override
    public Classification getClassificationByObjectAndColumn(final String objectName,
            final String columnName) {
        final Classification classification = this.odcmr.findByObjectNameAndColumnName(objectName,
                columnName);
        return classification;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED, readOnly = false, rollbackFor = {
            TransactionSystemException.class, DataIntegrityViolationException.class, ValidationError.class })
    public List<Classification> addClassifications(
            final List<Classification> classifications)
            throws ValidationError {
        final List<Classification> outputs = new ArrayList<Classification>();
        for (final Classification classification : classifications) {
            final List<Dataset> datasets = this.schemaMapUtil.getDatasetByObject(classification.getObjectName());
            final List<Long> datasetsIds = datasets.stream().map(dataset -> dataset.getStorageDataSetId())
                    .collect(Collectors.toList());
            classification.setDatasetIds(datasetsIds);
            final Classification newEntry = this.classificationUtil.addClassification(classification, sdf);
            final List<ClassificationMap> res = this.classificationUtil.addClassificationMap(classification, sdf);
            outputs.add(newEntry);
        }
        return outputs;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED, readOnly = false, rollbackFor = {
            TransactionSystemException.class, DataIntegrityViolationException.class, ValidationError.class })
    public Classification addClassification(final Classification classification)
            throws ValidationError {
        final Classification newEntry = this.classificationUtil.addClassification(classification, sdf);
        this.classificationUtil.addClassificationMap(classification, sdf);
        return newEntry;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED, readOnly = false, rollbackFor = {
            TransactionSystemException.class, DataIntegrityViolationException.class, ValidationError.class })
    public Classification editClassification(final Classification classification)
            throws ValidationError {
        final Classification editEntry = this.classificationUtil.editClassification(classification, sdf);
        this.classificationUtil.editClassificationMap(editEntry, sdf);
        return editEntry;
    }

    @Override
    public Classification updateClassification(
            final Classification datasetClassificationMap)
            throws ValidationError {
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final String time = sdf.format(timestamp);
        final Classification classification = this.odcmr
                .findById(datasetClassificationMap.getDatasetClassificationId()).orElse(null);

        if (classification == null) {
            final ValidationError verror = new ValidationError();
            verror.setErrorCode(HttpStatus.BAD_REQUEST);
            verror.setErrorDescription("Invalid Classification map Id");
            throw verror;
        }
        final String updatedUser = datasetClassificationMap.getUpdatedUser();
        classification.setClassificationId(datasetClassificationMap.getClassificationId());
        classification.setClassificationComment(datasetClassificationMap.getClassificationComment());
        classification.setUpdatedUser(updatedUser);
        classification.setUpdatedTimestamp(time);
        this.odcmr.save(classification);
        return classification;
    }

    @Override
    public Classification deActivateClassification(final long classificationDatasetMapId,
            final String updatedUser)
            throws ValidationError {
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final String time = sdf.format(timestamp);
        final Classification classification = this.odcmr
                .findById(classificationDatasetMapId).orElse(null);

        if (classification == null) {
            final ValidationError verror = new ValidationError();
            verror.setErrorCode(HttpStatus.BAD_REQUEST);
            verror.setErrorDescription("Invalid Classification map Id");
            throw verror;
        }
        classification.setUpdatedUser(updatedUser);
        classification.setUpdatedTimestamp(time);
        // classification.setIsActiveYN(ActiveEnumeration.NO.getFlag());
        this.odcmr.save(classification);
        return classification;
    }

    @Override
    public Classification reActivateClassification(final long classificationDatasetMapId,
            final String updatedUser)
            throws ValidationError {
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final String time = sdf.format(timestamp);
        final Classification classification = this.odcmr
                .findById(classificationDatasetMapId).orElse(null);

        if (classification == null) {
            final ValidationError verror = new ValidationError();
            verror.setErrorCode(HttpStatus.BAD_REQUEST);
            verror.setErrorDescription("Invalid Classification map Id");
            throw verror;
        }
        classification.setUpdatedUser(updatedUser);
        classification.setUpdatedTimestamp(time);
        // classification.setIsActiveYN(ActiveEnumeration.YES.getFlag());
        this.odcmr.save(classification);
        return classification;
    }

    @Override
    public Page<ClassificationDatasetMap> getClassificationMapByMiscAttributes(final String providerIds,
            final String entityIds,
            final String zoneIds, final String systemIds, final String classIds, final Pageable pageable) {
        final Pattern pattern = Pattern.compile(",");

        final List<Long> providerList = pattern.splitAsStream(providerIds)
                .map(Long::valueOf)
                .collect(Collectors.toList());
        final List<Long> classList = pattern.splitAsStream(classIds)
                .map(Long::valueOf)
                .collect(Collectors.toList());
        final List<Long> systemList = pattern.splitAsStream(systemIds)
                .map(Long::valueOf)
                .collect(Collectors.toList());
        final List<Long> entityList = pattern.splitAsStream(entityIds)
                .map(Long::valueOf)
                .collect(Collectors.toList());
        final List<Long> zoneList = pattern.splitAsStream(zoneIds)
                .map(Long::valueOf)
                .collect(Collectors.toList());

        final Page<Object[]> results = this.pageClassificationRepository.findByMiscAttributes(providerList, classList,
                systemList, entityList, zoneList, pageable);
        if (results == null) {
            final Page<ClassificationDatasetMap> emptyPage = new PageImpl<ClassificationDatasetMap>(
                    new ArrayList<ClassificationDatasetMap>(), pageable, 0);
            return emptyPage;
        }
        final List<Object[]> resultArray = results.getContent();
        final List<ClassificationDatasetMap> finalList = new ArrayList<ClassificationDatasetMap>();
        for (final Object[] obj : resultArray) {
            final long datasetClassificationId = ((Number) obj[0]).longValue();
            final String objectName = String.valueOf(obj[4]);
            final String columnName = String.valueOf(obj[5]);
            final long providerId = ((Number) obj[6]).longValue();
            final long classificationId = ((Number) obj[7]).longValue();
            final String classificationComment = String.valueOf(obj[8]);
            final String dataSource = String.valueOf(obj[2]);
            final String dataSourceType = String.valueOf(obj[1]);
            final String containerName = String.valueOf(obj[3]);
            final long storageDatasetId = ((Number) obj[13]).longValue();
            final long storageSystemId = ((Number) obj[14]).longValue();
            final long entityId = ((Number) obj[15]).longValue();
            final long zoneId = ((Number) obj[16]).longValue();
            final String createdUser = String.valueOf(obj[9]);
            final String createdTimestamp = String.valueOf(obj[10]);
            final String updatedUser = String.valueOf(obj[11]);
            final String updatedTimestamp = String.valueOf(obj[12]);

            final ClassificationDatasetMap cdm = new ClassificationDatasetMap(datasetClassificationId, objectName,
                    columnName, providerId, classificationId, classificationComment, dataSource, dataSourceType,
                    containerName, storageDatasetId, storageSystemId, entityId, zoneId, createdUser, createdTimestamp,
                    updatedUser, updatedTimestamp);
            finalList.add(cdm);
        }
        final Page<ClassificationDatasetMap> resultsPage = new PageImpl<ClassificationDatasetMap>(
                finalList, pageable, results.getTotalElements());

        return resultsPage;
    }

}
