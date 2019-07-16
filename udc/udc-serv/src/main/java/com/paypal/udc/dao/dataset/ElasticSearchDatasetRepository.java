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

package com.paypal.udc.dao.dataset;

import java.util.List;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import com.paypal.udc.entity.dataset.ElasticSearchDataset;


public interface ElasticSearchDatasetRepository extends ElasticsearchRepository<ElasticSearchDataset, String> {
    // Page<ElasticSearchDataset> findByStorageSystemIdIn(final List<Long> storageSystemIds, final Pageable pageable);

    List<ElasticSearchDataset> findByStorageDataSetName(final String storageDatasetName);

    List<ElasticSearchDataset> findByStorageDataSetId(final long storageDataSetId);

    Page<ElasticSearchDataset> findByStorageSystemIdInAndIsActiveYN(final List<Long> systemIds, String isActiveYN,
            Pageable pageable);

    // New Elastic Search Queries at dataset level and system level
    Page<ElasticSearchDataset> findByStorageDataSetNameContainingAndStorageSystemIdInAndIsActiveYN(
            final String datasetSubString,
            final List<Long> systemIds, final String isActiveYN, Pageable pageable);

    Page<ElasticSearchDataset> findByIsActiveYNAndStorageSystemIdInAndObjectAttributeValues_ObjectAttributeValueContainingOrIsActiveYNAndStorageSystemIdInAndCustomAttributeValues_ObjectAttributeValueContaining(
            String isActiveYN1, final List<Long> systemIds1, String attributeSubstring, String isActiveYN2,
            final List<Long> systemIds2, String customAttributeSubstring, Pageable pageable);

    Page<ElasticSearchDataset> findByObjectDescriptions_ObjectCommentContainingAndIsActiveYNAndStorageSystemIdIn(
            String descriptionSubstring, String isActiveYN, final List<Long> systemIds, Pageable pageable);

    Page<ElasticSearchDataset> findByObjectDescriptions_SchemaColumnDescriptions_ColumnCommentContainingOrObjectDescriptions_MiscColumnDescriptions_ColumnCommentContainingAndIsActiveYNAndStorageSystemIdIn(
            String columnDescription, String isActiveYN, final List<Long> systemIds, Pageable pageable);

    Page<ElasticSearchDataset> findByObjectSchema_ColumnNameContainingAndIsActiveYNAndStorageSystemIdIn(
            String columnName, String isActiveYN, final List<Long> systemIds, Pageable pageable);

    Page<ElasticSearchDataset> findByTags_TagNameContainingAndIsActiveYNAndStorageSystemIdIn(
            String tagName, String isActiveYN, final List<Long> systemIds, Pageable pageable);

    Page<ElasticSearchDataset> findByStorageSystemIdInAndIsActiveYNAndContainerName(List<Long> systemIds, String flag,
            String containerName, Pageable pageable);

    Page<ElasticSearchDataset> findByStorageDataSetNameContainingAndStorageSystemIdInAndIsActiveYNAndContainerName(
            String dataSetSubString, List<Long> systemIds, String flag, String containerName, Pageable pageable);

    Page<ElasticSearchDataset> findByTags_TagNameContainingAndIsActiveYNAndStorageSystemIdInAndContainerName(
            String dataSetSubString, String flag, List<Long> systemIds, String containerName, Pageable pageable);

    Page<ElasticSearchDataset> findByObjectSchema_ColumnNameContainingAndIsActiveYNAndStorageSystemIdInAndContainerName(
            String dataSetSubString, String flag, List<Long> systemIds, String containerName, Pageable pageable);

    Page<ElasticSearchDataset> findByObjectDescriptions_SchemaColumnDescriptions_ColumnCommentContainingOrObjectDescriptions_MiscColumnDescriptions_ColumnCommentContainingAndIsActiveYNAndStorageSystemIdInAndContainerName(
            String dataSetSubString, String flag, List<Long> systemIds, String containerName, Pageable pageable);

    Page<ElasticSearchDataset> findByObjectDescriptions_ObjectCommentContainingAndIsActiveYNAndContainerNameAndStorageSystemIdIn(
            String dataSetSubString, String flag, String containerName, List<Long> systemIds, Pageable pageable);

    Page<ElasticSearchDataset> findByIsActiveYNAndStorageSystemIdInAndContainerNameAndObjectAttributeValues_ObjectAttributeValueContainingOrIsActiveYNAndStorageSystemIdInAndContainerNameAndCustomAttributeValues_ObjectAttributeValueContaining(
            String flag, List<Long> systemIds, String containerName, String dataSetSubString, String flag2,
            List<Long> systemIds2, String containerName2, String dataSetSubString2, Pageable pageable);

}
