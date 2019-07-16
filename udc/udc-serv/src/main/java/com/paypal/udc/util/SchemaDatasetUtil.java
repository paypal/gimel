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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import com.paypal.udc.dao.integration.common.TagRepository;
import com.paypal.udc.dao.integration.schema.SchemaDatasetColumnMapRepository;
import com.paypal.udc.dao.integration.schema.SchemaDatasetMapRepository;
import com.paypal.udc.entity.dataset.DatasetWithAttributes;
import com.paypal.udc.entity.integration.common.DatasetColumnDescription;
import com.paypal.udc.entity.integration.common.DatasetDescription;
import com.paypal.udc.entity.integration.common.SourceProvider;
import com.paypal.udc.entity.integration.common.Tag;
import com.paypal.udc.entity.integration.schema.SchemaDatasetColumnMap;
import com.paypal.udc.entity.integration.schema.SchemaDatasetMap;
import com.paypal.udc.exception.ValidationError;


@Component
public class SchemaDatasetUtil {

    final static Logger logger = LoggerFactory.getLogger(SchemaDatasetUtil.class);

    @Autowired
    private SchemaDatasetMapRepository sdmr;

    @Autowired
    private SchemaDatasetColumnMapRepository schemaDatasetColumnMapRepository;

    @Autowired
    TagRepository tagRepository;

    @Autowired
    TagUtil tagUtil;

    @Autowired
    private ProviderUtil providerUtil;

    public List<SchemaDatasetColumnMap> getColumnsForSchemaDatasetId(final Long schemaDatasetMapId) {
        final List<SchemaDatasetColumnMap> schemaDatasetColumns = this.schemaDatasetColumnMapRepository
                .findBySchemaDatasetMapId(schemaDatasetMapId);
        return schemaDatasetColumns;
    }

    public SchemaDatasetMap getSchemaForDatasetBySystemContainerAndObject(final long systemId,
            final String containerName, final String objectName) {
        final List<SchemaDatasetMap> schemaDatasetMaps = this.sdmr.findByStorageSystemIdAndObjectNameAndContainerName(
                systemId, objectName, containerName);

        if (schemaDatasetMaps != null && schemaDatasetMaps.size() > 0) {
            final SchemaDatasetMap schemaDatasetMap = schemaDatasetMaps.stream()
                    .max(Comparator.comparing(SchemaDatasetMap::getUpdatedTimestamp))
                    .get();
            if (schemaDatasetMap != null) {
                final List<SchemaDatasetColumnMap> columnMap = this
                        .getColumnsForSchemaDatasetId(schemaDatasetMap.getSchemaDatasetMapId());
                schemaDatasetMap.setSchemaDatasetColumns(columnMap);
                return schemaDatasetMap;
            }
        }

        return new SchemaDatasetMap();
    }

    public List<SchemaDatasetMap> getSchemaForDataset(final long datasetId) {

        final List<SchemaDatasetMap> schemaDatasetMap = this.sdmr.findByStorageDatasetId(datasetId);
        schemaDatasetMap.forEach(description -> {
            final List<SchemaDatasetColumnMap> columnMap = this
                    .getColumnsForSchemaDatasetId(description.getSchemaDatasetMapId());
            description.setSchemaDatasetColumns(columnMap);
        });
        return schemaDatasetMap;

    }

    public void validateSchemaDatasetMapId(final long schemaDatasetMapId) throws ValidationError {
        final SchemaDatasetMap schemaDatasetMap = this.sdmr.findById(schemaDatasetMapId).orElse(null);
        if (schemaDatasetMap == null) {
            final ValidationError verror = new ValidationError();
            verror.setErrorCode(HttpStatus.BAD_REQUEST);
            verror.setErrorDescription("Invalid SchemaDatasetMapId");
            throw verror;
        }
    }

    public void setTeradataSchemaAndTagsForDataset(final DatasetWithAttributes dataset,
            final long storageSystemId, final String containerName, final String objectName) throws ValidationError {

        final SchemaDatasetMap schemaDatasetMap = this.getSchemaForDatasetBySystemContainerAndObject(storageSystemId,
                containerName, objectName);
        if (schemaDatasetMap.getSchemaDatasetMapId() != 0) {
            final List<SchemaDatasetColumnMap> columnDescriptions = schemaDatasetMap.getSchemaDatasetColumns();
            final List<DatasetColumnDescription> datasetColumnDescriptions = new ArrayList<DatasetColumnDescription>();
            if (columnDescriptions != null && columnDescriptions.size() > 0) {
                columnDescriptions.forEach(columnDescription -> {
                    final DatasetColumnDescription datasetColumnDescription = new DatasetColumnDescription(
                            columnDescription.getColumnName(), columnDescription.getColumnComment(),
                            columnDescription.getCreatedTimestamp(), columnDescription.getUpdatedTimestamp());
                    datasetColumnDescriptions.add(datasetColumnDescription);
                });
            }
            final SourceProvider provider = this.providerUtil.validateProvider(schemaDatasetMap.getProviderId());
            final String providerName = provider.getSourceProviderName().equals("USER")
                    ? schemaDatasetMap.getCreatedUser() : provider.getSourceProviderName();
            final DatasetDescription datasetDescription = new DatasetDescription(
                    dataset.getStorageDataSetName(), schemaDatasetMap.getObjectComment(),
                    datasetColumnDescriptions, providerName);
            dataset.setSchemaDescription(datasetDescription);
            final List<Tag> tags = this.tagUtil.getTagsByDatasetId(schemaDatasetMap.getStorageDatasetId());
            dataset.setTags(tags);
        }
        else {
            final List<Tag> tags = this.tagUtil.getTagsByDatasetId(dataset.getStorageDataSetId());
            dataset.setTags(tags);
        }

    }
}
