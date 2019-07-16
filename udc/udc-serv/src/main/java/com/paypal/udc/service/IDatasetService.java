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

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import com.paypal.udc.entity.dataset.CumulativeDataset;
import com.paypal.udc.entity.dataset.Dataset;
import com.paypal.udc.entity.dataset.DatasetChangeLog;
import com.paypal.udc.entity.dataset.DatasetWithAttributes;
import com.paypal.udc.entity.dataset.ElasticSearchDataset;
import com.paypal.udc.exception.ValidationError;


public interface IDatasetService {

    List<CumulativeDataset> getAllDatasets(final String dataSetSubString);

    Dataset getDataSetById(long datasetId) throws ValidationError;

    DatasetWithAttributes getDataSetByName(String dataSetName) throws ValidationError;

    Dataset updateDataSet(Dataset dataSet)
            throws ValidationError, IOException, InterruptedException, ExecutionException;

    Dataset deleteDataSet(long dataSetId) throws ValidationError, IOException, InterruptedException, ExecutionException;

    List<DatasetChangeLog> getChangeLogsByDataSetId(long datasetId);

    List<DatasetChangeLog> getChangeLogsByDataSetIdAndChangeColumnType(long datasetId, String changeType);

    Page<Dataset> getAllDatasetsByTypeAndSystem(final String datasetSubstring, final String storageTypeName,
            final String storageSystemName, final String zoneName, final Pageable pageable) throws ValidationError;

    Dataset addDataset(Dataset dataset) throws ValidationError, IOException, InterruptedException, ExecutionException;

    Page<Dataset> getPendingDatasets(final String datasetSubstring, final String storageTypeName,
            final String storageSystemName, final Pageable pageable) throws ValidationError;

    void updateDatasetWithPendingAttributes(final DatasetWithAttributes dataset) throws ValidationError;

    DatasetWithAttributes getPendingDataset(final long dataSetId) throws ValidationError;

    Page<Dataset> getAllDeletedDatasetsByTypeAndSystem(final String datasetSubstring, String storageTypeName,
            String storageSystemName, Pageable pageable) throws ValidationError;

    long getDatasetCount();

    List<CumulativeDataset> getAllDetailedDatasets(String dataSetSubString);

    // Page<ElasticSearchDataset> getAllDatasetsFromES(String dataSetSubString, final Pageable pageable,
    // final String storageTypeName, final String storageSystemName, final String zoneName) throws ValidationError;

    Map<String, List<String>> getTimelineDimensions();

    Page<ElasticSearchDataset> getAllDatasetsFromESByType(String dataSetSubString, Pageable pageable, String searchType,
            String storageSystemList, final String containerName) throws ValidationError;
}
