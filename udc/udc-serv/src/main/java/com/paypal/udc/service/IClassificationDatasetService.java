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

package com.paypal.udc.service;

import java.util.List;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import com.paypal.udc.entity.classification.Classification;
import com.paypal.udc.entity.classification.ClassificationDatasetMap;
import com.paypal.udc.exception.ValidationError;


public interface IClassificationDatasetService {

    Page<Classification> getAllClassifications(final Pageable pageable) throws ValidationError;

    Page<Classification> getClassificationByClassId(final Pageable pageable, long classificationId)
            throws ValidationError;

    List<Classification> getClassificationByObjectName(final String objectName);

    List<Classification> getClassificationByColumnName(final String columnName);

    Classification getClassificationByObjectAndColumn(final String objectName, final String columnName);

    Classification addClassification(Classification classification)
            throws ValidationError;

    List<Classification> addClassifications(List<Classification> classifications)
            throws ValidationError;

    Classification editClassification(Classification classification)
            throws ValidationError;

    Classification updateClassification(Classification datasetClassificationMap)
            throws ValidationError;

    Classification deActivateClassification(final long classificationDatasetMapId,
            final String updatedUser)
            throws ValidationError;

    Classification reActivateClassification(final long classificationDatasetMapId,
            final String updatedUser)
            throws ValidationError;

    Page<ClassificationDatasetMap> getClassificationMapByMiscAttributes(String providerIds, String entityIds,
            String zoneIds, String systemIds, String classIds, Pageable pageable);

}
