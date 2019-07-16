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

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.paypal.udc.dao.dataset.DatasetRepository;
import com.paypal.udc.dao.dataset.DatasetStorageSystemRepository;
import com.paypal.udc.dao.dataset.ElasticSearchDatasetRepository;
import com.paypal.udc.dao.dataset.PageableDatasetRepository;
import com.paypal.udc.dao.integration.description.DatasetColumnDescriptionMapRepository;
import com.paypal.udc.dao.integration.description.DatasetDescriptionMapRepository;
import com.paypal.udc.dao.integration.schema.SchemaDatasetColumnMapRepository;
import com.paypal.udc.dao.integration.schema.SchemaDatasetMapRepository;
import com.paypal.udc.dao.objectschema.ObjectSchemaAttributeCustomKeyValueRepository;
import com.paypal.udc.dao.objectschema.ObjectSchemaAttributeValueRepository;
import com.paypal.udc.dao.storagesystem.StorageSystemAttributeValueRepository;
import com.paypal.udc.dao.storagesystem.StorageSystemRepository;
import com.paypal.udc.dao.storagetype.ElasticSearchStorageTypeRepository;
import com.paypal.udc.dao.storagetype.StorageTypeAttributeKeyRepository;
import com.paypal.udc.dao.teradatapolicy.TeradataPolicyRepository;
import com.paypal.udc.dao.zone.ElasticSearchZoneRepository;
import com.paypal.udc.entity.cluster.Cluster;
import com.paypal.udc.entity.dataset.Dataset;
import com.paypal.udc.entity.dataset.DatasetStorageSystem;
import com.paypal.udc.entity.dataset.DatasetWithAttributes;
import com.paypal.udc.entity.dataset.ElasticSearchDataset;
import com.paypal.udc.entity.entity.Entity;
import com.paypal.udc.entity.integration.common.Tag;
import com.paypal.udc.entity.integration.description.DatasetColumnDescriptionMap;
import com.paypal.udc.entity.integration.description.DatasetDescriptionMap;
import com.paypal.udc.entity.integration.description.SourceProviderDatasetMap;
import com.paypal.udc.entity.integration.schema.SchemaDatasetColumnMap;
import com.paypal.udc.entity.integration.schema.SchemaDatasetMap;
import com.paypal.udc.entity.objectschema.ObjectAttributeKeyValue;
import com.paypal.udc.entity.objectschema.ObjectAttributeValue;
import com.paypal.udc.entity.objectschema.ObjectSchemaMap;
import com.paypal.udc.entity.objectschema.Schema;
import com.paypal.udc.entity.ownership.DatasetOwnershipMap;
import com.paypal.udc.entity.rangerpolicy.DerivedPolicy;
import com.paypal.udc.entity.storagesystem.StorageSystem;
import com.paypal.udc.entity.storagesystem.StorageSystemAttributeValue;
import com.paypal.udc.entity.storagetype.ElasticSearchStorageType;
import com.paypal.udc.entity.storagetype.StorageType;
import com.paypal.udc.entity.storagetype.StorageTypeAttributeKey;
import com.paypal.udc.entity.teradatapolicy.TeradataPolicy;
import com.paypal.udc.entity.zone.ElasticSearchZone;
import com.paypal.udc.entity.zone.Zone;
import com.paypal.udc.exception.ValidationError;
import com.paypal.udc.util.enumeration.ActiveEnumeration;
import com.paypal.udc.util.enumeration.SearchFieldEnumeration;


@Component
public class DatasetUtil {

    final static Logger logger = LoggerFactory.getLogger(DatasetUtil.class);
    static final String teradataType = "teradata";
    static final String hbaseType = "hbase";
    static final String hiveType = "hive";

    // search filters for elastic search

    @Autowired
    private TransportClient client;

    @Autowired
    private ElasticSearchDatasetRepository esDatasetRepository;

    @Autowired
    private SchemaDatasetUtil schemaDatasetUtil;

    @Autowired
    private DatasetStorageSystemRepository datasetSystemRepository;

    @Autowired
    private StorageSystemRepository storageSystemRepository;

    @Autowired
    private TagUtil tagUtil;

    @Autowired
    private OwnershipUtil ownershipUtil;

    @Autowired
    private StorageTypeAttributeKeyRepository storageTypeAttributeRepository;

    @Autowired
    private DatasetRepository dataSetRepository;

    @Autowired
    private PageableDatasetRepository pageDatasetRepository;

    @Autowired
    private StorageSystemUtil systemUtil;

    @Autowired
    private StorageTypeUtil storageTypeUtil;

    @Autowired
    private ZoneUtil zoneUtil;

    @Autowired
    private StorageSystemAttributeValueRepository systemAttributeValueRepository;

    @Autowired
    private TeradataPolicyRepository teradataPolicyRepository;

    @Autowired
    private ObjectSchemaAttributeValueRepository objectSchemaAttributeRepository;

    @Autowired
    private RangerPolicyUtil rangerPolicyUtil;

    @Autowired
    private ClusterUtil clusterUtil;

    @Autowired
    private ObjectSchemaMapUtil schemaMapUtil;

    @Autowired
    private DatasetDescriptionUtil bodhiDatasetUtil;

    @Autowired
    private SchemaDatasetMapRepository schemaDatasetMapRepository;

    @Autowired
    private SchemaDatasetColumnMapRepository schemaDatasetColumnMapRepository;

    @Autowired
    private DatasetDescriptionMapRepository datasetDescriptionMapRepository;

    @Autowired
    private DatasetColumnDescriptionMapRepository datasetColumnDescriptionMapRepository;

    @Autowired
    private ObjectSchemaAttributeCustomKeyValueRepository sackvr;

    @Autowired
    private ElasticSearchStorageTypeRepository esTypeRepository;

    @Autowired
    private ElasticSearchZoneRepository esZoneRepository;

    @Autowired
    private EntityUtil entityUtil;

    private final List<String> restrictedColumns = new ArrayList<String>();

    public Dataset getDataSet(final Dataset dataSet) {
        final long datasetId = dataSet.getStorageDataSetId();
        final DatasetStorageSystem dsw = this.datasetSystemRepository.findByStorageDataSetId(datasetId);
        dataSet.setStorageSystemId(dsw.getStorageSystemId());
        return dataSet;
    }

    public Map<Long, DatasetStorageSystem> getDatasetStorageSystemMappings() {
        final Map<Long, DatasetStorageSystem> mappings = new HashMap<Long, DatasetStorageSystem>();
        final List<DatasetStorageSystem> datasetSystems = new ArrayList<DatasetStorageSystem>();
        this.datasetSystemRepository.findAll().forEach(datasetSystems::add);
        datasetSystems.forEach(datasetsystem -> {
            mappings.put(datasetsystem.getStorageDataSetId(), datasetsystem);
        });
        return mappings;
    }

    public Map<Long, DatasetStorageSystem> getDatasetStorageSystemMappings(final List<Long> storageSystemIds,
            final String isActiveYN) {
        final Map<Long, DatasetStorageSystem> mappings = new HashMap<Long, DatasetStorageSystem>();
        final List<DatasetStorageSystem> datasetSystems = new ArrayList<DatasetStorageSystem>();
        this.datasetSystemRepository.findByStorageSystemIdInAndIsActiveYN(storageSystemIds, isActiveYN)
                .forEach(datasetSystems::add);
        datasetSystems.forEach(datasetsystem -> {
            mappings.put(datasetsystem.getStorageDataSetId(), datasetsystem);
        });

        return mappings;
    }

    public Map<Long, String> getDataSetSchemaMapping() {
        final Map<Long, String> dataSetSchemaMap = new HashMap<Long, String>();
        final List<Object[]> schemas = this.dataSetRepository.findDatasetSchema();

        for (final Object[] schema : schemas) {
            final Integer i = (Integer) schema[0];
            dataSetSchemaMap.put(Long.valueOf(i), (String) schema[1]);
        }

        return dataSetSchemaMap;
    }

    public boolean validateAttributes(final long objectSchemaMapId, final long storageSystemId,
            final long storageTypeId) {
        final List<Long> objectAttributes = this.objectSchemaAttributeRepository
                .findByObjectIdAndIsActiveYN(objectSchemaMapId, ActiveEnumeration.YES.getFlag()).stream()
                .map(attribute -> attribute.getStorageDsAttributeKeyId()).collect(Collectors.toList());
        final List<Long> systemAttributes = this.systemUtil.getAttributes(storageSystemId).stream()
                .map(attribute -> attribute.getStorageDataSetAttributeKeyId()).collect(Collectors.toList());

        final Collection<Long> combinedAttributes = Stream.of(objectAttributes, systemAttributes)
                .flatMap(Collection::stream).collect(Collectors.toList());

        final List<Long> typeAttributes = this.storageTypeAttributeRepository
                .findByStorageTypeIdAndIsActiveYN(storageTypeId, ActiveEnumeration.YES.getFlag()).stream()
                .map(typeAttribute -> typeAttribute.getStorageDsAttributeKeyId()).collect(Collectors.toList());

        if (combinedAttributes.size() != typeAttributes.size() || !combinedAttributes.containsAll(typeAttributes)) {
            return false;
        }
        return true;

    }

    public List<Long> populateMissingAttributes(final long objectSchemaMapId, final long storageSystemId,
            final long storageTypeId) {
        final List<Long> objectAttributes = this.objectSchemaAttributeRepository
                .findByObjectIdAndIsActiveYN(objectSchemaMapId, ActiveEnumeration.YES.getFlag()).stream()
                .map(attribute -> attribute.getStorageDsAttributeKeyId()).collect(Collectors.toList());
        final List<Long> systemAttributes = this.systemUtil.getAttributes(storageSystemId).stream()
                .map(attribute -> attribute.getStorageDataSetAttributeKeyId()).collect(Collectors.toList());

        final Collection<Long> combinedAttributes = Stream.of(objectAttributes, systemAttributes)
                .flatMap(Collection::stream).collect(Collectors.toList());

        final List<Long> typeAttributes = this.storageTypeAttributeRepository
                .findByStorageTypeIdAndIsActiveYN(storageTypeId, ActiveEnumeration.YES.getFlag()).stream()
                .map(typeAttribute -> typeAttribute.getStorageDsAttributeKeyId()).collect(Collectors.toList());
        if (combinedAttributes.size() == typeAttributes.size() || combinedAttributes.containsAll(typeAttributes)) {
            return new ArrayList<Long>();
        }
        else {
            return typeAttributes.stream()
                    .filter(str1 -> !combinedAttributes.stream().map(x -> x).collect(Collectors.toSet())
                            .contains(str1))
                    .collect(Collectors.toList());
        }
    }

    public DatasetWithAttributes populateDatasetWithAttributes(final Dataset ds,
            final Map<Long, DatasetStorageSystem> dswMappings, final Map<Long, StorageSystem> systemMappings,
            final Map<Long, StorageTypeAttributeKey> typeAttributeMap) throws ValidationError {

        final DatasetWithAttributes dataset = new DatasetWithAttributes();
        final Gson gson = new Gson();
        dataset.setStorageDataSetId(ds.getStorageDataSetId());
        dataset.setStorageDataSetName(ds.getStorageDataSetName());
        dataset.setStorageDataSetAliasName(ds.getStorageDataSetAliasName());
        dataset.setObjectSchemaMapId(ds.getObjectSchemaMapId());
        dataset.setIsAutoRegistered(ds.getIsAutoRegistered());
        dataset.setCreatedTimestamp(ds.getCreatedTimestamp());
        dataset.setUpdatedTimestamp(ds.getUpdatedTimestamp());
        dataset.setCreatedUser(ds.getCreatedUser());
        dataset.setUpdatedUser(ds.getUpdatedUser());

        final DatasetStorageSystem dsw = dswMappings.get(ds.getStorageDataSetId());
        final long storageSystemId = dsw.getStorageSystemId();
        dataset.setStorageSystemId(storageSystemId);
        final ObjectSchemaMap rawObject = this.schemaMapUtil
                .validateObjectSchemaMapId(dataset.getObjectSchemaMapId());

        dataset.setQuery(rawObject.getQuery());
        final String schema = rawObject.getObjectSchemaInString();
        final String storageSystemName = systemMappings.get(storageSystemId).getStorageSystemName();
        final List<Schema> modifiedSchema = gson.fromJson(schema, new TypeToken<List<Schema>>() {
        }.getType());
        dataset.setObjectSchema(modifiedSchema);
        dataset.setStorageSystemName(storageSystemName);

        final List<StorageSystemAttributeValue> systemAttributes = this.systemAttributeValueRepository
                .findByStorageSystemIdAndIsActiveYN(storageSystemId,
                        ActiveEnumeration.YES.getFlag());
        dataset.setSystemAttributes(systemAttributes);

        final List<ObjectAttributeValue> objectAttributes = this.objectSchemaAttributeRepository
                .findByObjectIdAndIsActiveYN(ds.getObjectSchemaMapId(), ActiveEnumeration.YES.getFlag());
        objectAttributes.forEach(attribute -> {
            final StorageTypeAttributeKey stak = typeAttributeMap.get(attribute.getStorageDsAttributeKeyId());
            attribute.setStorageDsAttributeKeyName(stak.getStorageDsAttributeKeyName());
        });
        dataset.setObjectAttributes(objectAttributes);

        final List<Long> pendingAttributeIds = this.populateMissingAttributes(ds.getObjectSchemaMapId(),
                storageSystemId, systemMappings.get(storageSystemId).getStorageTypeId());
        final List<StorageTypeAttributeKey> pendingTypeAttributes = pendingAttributeIds.stream()
                .map(pendingAttr -> typeAttributeMap.get(pendingAttr))
                .collect(Collectors.toList());
        dataset.setPendingTypeAttributes(pendingTypeAttributes);

        if (storageSystemName.contains("Teradata")) {
            this.schemaDatasetUtil.setTeradataSchemaAndTagsForDataset(dataset, storageSystemId,
                    rawObject.getContainerName(), rawObject.getObjectName());

        }
        else {
            this.bodhiDatasetUtil.setDatasetSchemaAndTagsForDataset(dataset, storageSystemId,
                    rawObject.getContainerName(), rawObject.getObjectName());
        }

        return dataset;
    }

    public Page<Dataset> getPendingDatasetsForAllTypesAndSystems(final String datasetStr, final Pageable pageable)
            throws ValidationError {

        final String datasetSubString = datasetStr.equals("All") ? "" : datasetStr;
        final Map<Long, StorageSystem> systemMappings = this.systemUtil.getStorageSystems();
        final Page<Dataset> pagedDatasets = this.pageDatasetRepository
                .findByIsActiveYNAndStorageDataSetNameContaining(ActiveEnumeration.YES.getFlag(), datasetSubString,
                        pageable);

        final List<Dataset> listDatasets = pagedDatasets.getContent();

        for (final Dataset dataset : listDatasets) {
            final ObjectSchemaMap schemaMap = this.schemaMapUtil
                    .validateObjectSchemaMapId(dataset.getObjectSchemaMapId());
            final long storageSystemId = schemaMap.getStorageSystemId();
            final StorageSystem storageSystem = systemMappings.get(storageSystemId);
            final StorageType storageType = this.systemUtil.getStorageType(storageSystemId);
            this.populatePendingDataset(storageSystem, dataset, storageType);
            dataset.setStorageSystemId(storageSystemId);
        }
        return pagedDatasets;

    }

    public Page<Dataset> getAllDeletedDatasetsForSystem(final String datasetStr, final String storageSystemName,
            final Pageable pageable) throws ValidationError {
        final StorageSystem storageSystem = this.storageSystemRepository
                .findByStorageSystemName(storageSystemName);
        final StorageType storageType = this.storageTypeUtil.validateStorageTypeId(storageSystem.getStorageTypeId());
        final List<Long> datasetIds = this.datasetSystemRepository
                .findByStorageSystemIdAndIsActiveYN(storageSystem.getStorageSystemId(), ActiveEnumeration.NO.getFlag())
                .stream().map(dss -> dss.getStorageDataSetId()).collect(Collectors.toList());
        final String datasetSubstring = datasetStr.equals("All") ? "" : datasetStr;
        final Page<Dataset> pagedDatasets = this.pageDatasetRepository
                .findByStorageDataSetIdInAndStorageDataSetNameContaining(datasetIds, datasetSubstring, pageable);
        pagedDatasets.getContent().stream().forEach(dataset -> {
            this.populateAvailableDataset(storageSystem, dataset, storageType);
        });
        return pagedDatasets;
    }

    public Page<Dataset> getAllDeletedDatasetsForSystemIds(final String datasetStr, final List<Long> storageSystemIds,
            final Pageable pageable, final StorageType storageType) {
        final Map<Long, DatasetStorageSystem> datasetsForStorageSystems = this
                .getDatasetStorageSystemMappings(storageSystemIds, ActiveEnumeration.NO.getFlag());
        final Map<Long, StorageSystem> systemMap = this.systemUtil.getStorageSystems();
        final List<Long> datasetIds = datasetsForStorageSystems.keySet().stream()
                .collect(Collectors.toList());

        final String datasetSubstring = datasetStr.equals("All") ? "" : datasetStr;
        final Page<Dataset> pagedDatasets = this.pageDatasetRepository
                .findByStorageDataSetIdInAndStorageDataSetNameContaining(datasetIds, datasetSubstring, pageable);
        pagedDatasets.getContent().stream().forEach(dataset -> {
            final DatasetStorageSystem datasetSystem = datasetsForStorageSystems.get(dataset.getStorageDataSetId());
            final StorageSystem storageSystem = systemMap.get(datasetSystem.getStorageSystemId());
            this.populateAvailableDataset(storageSystem, dataset, storageType);
        });

        return pagedDatasets;
    }

    public Page<Dataset> getDatasetsForAllTypesAndSystems(final String datasetStr, final Pageable pageable)
            throws ValidationError {
        final Map<Long, Zone> zones = this.zoneUtil.getZones();
        final Map<Long, StorageSystem> systemMap = this.systemUtil.getStorageSystems();
        final String datasetSubString = datasetStr.equals("All") ? "" : datasetStr;
        final Page<Dataset> pagedDatasets = this.pageDatasetRepository
                .findByIsActiveYNAndStorageDataSetNameContaining(ActiveEnumeration.YES.getFlag(), datasetSubString,
                        pageable);

        final List<Dataset> listDatasets = pagedDatasets.getContent();

        for (final Dataset dataset : listDatasets) {
            final ObjectSchemaMap schemaMap = this.schemaMapUtil
                    .validateObjectSchemaMapId(dataset.getObjectSchemaMapId());
            final long storageSystemId = schemaMap.getStorageSystemId();
            final StorageSystem storageSystem = systemMap.get(storageSystemId);
            final StorageType storageType = this.systemUtil.getStorageType(storageSystemId);
            this.populateAvailableDataset(storageSystem, dataset, storageType);
            dataset.setStorageSystemId(storageSystemId);
            this.setAccessControlForDataset(storageType.getStorageTypeName().toLowerCase(), storageSystem,
                    schemaMap.getObjectId(), dataset);

            dataset.setIsReadCompatible(storageSystem.getIsReadCompatible());
            dataset.setZoneName(zones.get(storageSystem.getZoneId()).getZoneName());
        }
        return pagedDatasets;
    }

    public Page<Dataset> getAllDatasetForSystem(final String datasetStr, final String storageSystemName,
            final Pageable pageable) throws ValidationError {
        final String datasetSubstring = datasetStr.equals("All") ? "" : datasetStr;
        final Page<Dataset> pagedDatasets;
        final StorageSystem storageSystem = this.storageSystemRepository.findByStorageSystemName(storageSystemName);
        final StorageType storageType = this.storageTypeUtil.validateStorageTypeId(storageSystem.getStorageTypeId());
        if (datasetSubstring.equals("")) {
            pagedDatasets = this.pageDatasetRepository.findByIsActiveYNAndStorageSystemId(
                    ActiveEnumeration.YES.getFlag(), storageSystem.getStorageSystemId(), pageable);
        }
        else {
            pagedDatasets = this.pageDatasetRepository
                    .findByIsActiveYNAndStorageDataSetNameContainingAndStorageSystemId(
                            ActiveEnumeration.YES.getFlag(), datasetSubstring, storageSystem.getStorageSystemId(),
                            pageable);
        }

        final List<Dataset> listDatasets = pagedDatasets.getContent();
        for (final Dataset dataset : listDatasets) {
            final ObjectSchemaMap objectSchemaMap = this.schemaMapUtil
                    .validateObjectSchemaMapId(dataset.getObjectSchemaMapId());
            this.setAccessControlForDataset(storageType.getStorageTypeName().toLowerCase(),
                    storageSystem, objectSchemaMap.getObjectId(), dataset);
            this.populateAvailableDataset(storageSystem, dataset, storageType);
        }
        return pagedDatasets;

    }

    public Page<Dataset> getAllDatasetForSystemIds(final String datasetStr, final List<StorageSystem> storageSystems,
            final Pageable pageable, final StorageType storageType) throws ValidationError {
        final String datasetSubstring = datasetStr.equals("All") ? "" : datasetStr;
        final String storageTypeName = storageType.getStorageTypeName().toLowerCase();
        final Page<Dataset> pagedDatasets;
        final List<Long> storageSystemIds = storageSystems.stream()
                .map(storageSystem -> storageSystem.getStorageSystemId()).collect(Collectors.toList());
        if (datasetSubstring.equals("")) {
            pagedDatasets = this.pageDatasetRepository.findByIsActiveYNAndStorageSystemIdIn(
                    ActiveEnumeration.YES.getFlag(), storageSystemIds, pageable);
        }
        else {
            pagedDatasets = this.pageDatasetRepository
                    .findByIsActiveYNAndStorageDataSetNameContainingAndStorageSystemIdIn(
                            ActiveEnumeration.YES.getFlag(), datasetSubstring, pageable, storageSystemIds);
        }
        final List<Dataset> listDatasets = pagedDatasets.getContent();
        for (final Dataset dataset : listDatasets) {
            final ObjectSchemaMap objectSchemaMap = this.schemaMapUtil
                    .validateObjectSchemaMapId(dataset.getObjectSchemaMapId());
            final StorageSystem storageSystem = storageSystems.stream()
                    .filter(ss -> ss.getStorageSystemId() == objectSchemaMap.getStorageSystemId())
                    .collect(Collectors.toList()).get(0);
            this.setAccessControlForDataset(storageTypeName, storageSystem, objectSchemaMap.getObjectId(),
                    dataset);
            this.populateAvailableDataset(storageSystem, dataset, storageType);
        }
        return pagedDatasets;
    }

    public Page<Dataset> getAllDatasetForZoneSystemIds(final String datasetStr,
            final List<StorageSystem> storageSystems,
            final Pageable pageable) throws ValidationError {

        final String datasetSubstring = datasetStr.equals("All") ? "" : datasetStr;
        final Page<Dataset> pagedDatasets;
        final List<Long> storageSystemIds = storageSystems.stream()
                .map(storageSystem -> storageSystem.getStorageSystemId()).collect(Collectors.toList());
        if (datasetSubstring.equals("")) {
            pagedDatasets = this.pageDatasetRepository.findByIsActiveYNAndStorageSystemIdIn(
                    ActiveEnumeration.YES.getFlag(), storageSystemIds, pageable);
        }
        else {
            pagedDatasets = this.pageDatasetRepository
                    .findByIsActiveYNAndStorageDataSetNameContainingAndStorageSystemIdIn(
                            ActiveEnumeration.YES.getFlag(), datasetSubstring, pageable, storageSystemIds);
        }
        final List<Dataset> listDatasets = pagedDatasets.getContent();
        for (final Dataset dataset : listDatasets) {
            final ObjectSchemaMap objectSchemaMap = this.schemaMapUtil
                    .validateObjectSchemaMapId(dataset.getObjectSchemaMapId());
            final StorageSystem storageSystem = storageSystems.stream()
                    .filter(ss -> ss.getStorageSystemId() == objectSchemaMap.getStorageSystemId())
                    .collect(Collectors.toList()).get(0);
            final StorageType storageType2 = this.systemUtil.getStorageType(storageSystem.getStorageSystemId());
            this.setAccessControlForDataset(storageType2.getStorageTypeName(), storageSystem,
                    objectSchemaMap.getObjectId(), dataset);
            this.populateAvailableDataset(storageSystem, dataset, storageType2);
        }

        return pagedDatasets;
    }

    public void setAccessControlForDataset(final String storageTypeName, final StorageSystem storageSystem,
            final long objectId, final Dataset dataset) {
        switch (storageTypeName.toLowerCase()) {
            case teradataType:
                this.setAccessForTeradataDataset(dataset, storageSystem.getStorageSystemId(), objectId);
                break;
            case hbaseType:
                this.setAccessForHbaseDataset(dataset, storageSystem.getRunningClusterId(), storageTypeName,
                        objectId);
                break;
            case hiveType:
                this.setAccessForHiveDataset(dataset, storageSystem.getRunningClusterId(), objectId);
                break;
            default:
                dataset.setIsAccessControlled(ActiveEnumeration.NO.getFlag());
                break;
        }
    }

    private void setAccessForHbaseDataset(final Dataset dataset, final long clusterId, final String storageTypeName,
            final long objectId) {
        final List<ObjectAttributeValue> objectAttributes = this.objectSchemaAttributeRepository
                .findByObjectIdAndIsActiveYN(objectId, ActiveEnumeration.YES.getFlag());
        final List<DerivedPolicy> allPolicies = new ArrayList<DerivedPolicy>();
        final List<TeradataPolicy> teradataPolicies = new ArrayList<TeradataPolicy>();
        if (objectAttributes == null || objectAttributes.size() == 0) {
            dataset.setIsAccessControlled(ActiveEnumeration.NO.getFlag());
        }
        else {
            for (final ObjectAttributeValue objectAttribute : objectAttributes) {
                final List<DerivedPolicy> derivedPolicies = this.rangerPolicyUtil
                        .computePoliciesByLocation("", storageTypeName,
                                clusterId, objectAttribute.getObjectAttributeValue(), "");
                if (derivedPolicies != null && derivedPolicies.size() > 0) {
                    allPolicies.addAll(derivedPolicies);
                    dataset.setIsAccessControlled(ActiveEnumeration.YES.getFlag());
                    break;
                }
            }
            if (dataset.getIsAccessControlled() == null) {
                dataset.setIsAccessControlled(ActiveEnumeration.NO.getFlag());
            }
        }
        dataset.setDerivedPolicies(allPolicies);
        dataset.setTeradataPolicies(teradataPolicies);

    }

    private void setAccessForHiveDataset(final Dataset dataset, final long clusterId,
            final long objectId) {
        final Map<Long, Cluster> clusters = this.clusterUtil.getClusters();
        final String clusterName = clusters.get(clusterId).getClusterName();
        final List<DerivedPolicy> allPolicies = new ArrayList<DerivedPolicy>();
        final List<TeradataPolicy> teradataPolicies = new ArrayList<TeradataPolicy>();
        final List<ObjectAttributeValue> objectAttributes = this.objectSchemaAttributeRepository
                .findByObjectIdAndIsActiveYN(objectId, ActiveEnumeration.YES.getFlag());
        final String stringToReplace = "hdfs://" + clusterName;
        if (objectAttributes == null || objectAttributes.size() == 0) {
            dataset.setIsAccessControlled(ActiveEnumeration.NO.getFlag());

        }
        else {
            for (final ObjectAttributeValue objectAttribute : objectAttributes) {
                final String attributeValue = objectAttribute.getObjectAttributeValue().replace(stringToReplace, "");
                if (attributeValue.contains("/")) {
                    final List<DerivedPolicy> derivedPolicies = this.rangerPolicyUtil
                            .computePoliciesByLocation(attributeValue, "hdfs", clusterId, "", "");
                    if (derivedPolicies != null && derivedPolicies.size() > 0) {
                        allPolicies.addAll(derivedPolicies);
                        dataset.setIsAccessControlled(ActiveEnumeration.YES.getFlag());
                        break;
                    }
                }
            }
            if (dataset.getIsAccessControlled() == null) {
                dataset.setIsAccessControlled(ActiveEnumeration.NO.getFlag());
            }
        }
        dataset.setDerivedPolicies(allPolicies);
        dataset.setTeradataPolicies(teradataPolicies);
    }

    private void setAccessForTeradataDataset(final Dataset dataset, final long storageSystemId,
            final long objectId) {
        final List<ObjectAttributeValue> objectAttributes = this.objectSchemaAttributeRepository
                .findByObjectIdAndIsActiveYN(objectId,
                        ActiveEnumeration.YES.getFlag());
        final List<TeradataPolicy> allPolicies = new ArrayList<TeradataPolicy>();
        final List<DerivedPolicy> derivedPolicies = new ArrayList<DerivedPolicy>();
        if (objectAttributes == null || objectAttributes.size() == 0) {
            dataset.setIsAccessControlled(ActiveEnumeration.NO.getFlag());
        }
        else {
            for (final ObjectAttributeValue objectAttribute : objectAttributes) {
                final String databaseName = objectAttribute.getObjectAttributeValue().split("\\.")[0];
                final List<TeradataPolicy> policies = this.teradataPolicyRepository
                        .findByStorageSystemIdAndDatabaseName(storageSystemId, databaseName);
                if (policies != null && policies.size() > 0) {
                    allPolicies.addAll(policies);
                    dataset.setIsAccessControlled(ActiveEnumeration.YES.getFlag());
                    break;
                }
            }
            if (dataset.getIsAccessControlled() == null) {
                dataset.setIsAccessControlled(ActiveEnumeration.NO.getFlag());
            }
        }
        dataset.setTeradataPolicies(allPolicies);
        dataset.setDerivedPolicies(derivedPolicies);
    }

    public Page<Dataset> getDeletedDatasetsForAllTypesAndSystems(final String datasetStr, final Pageable pageable)
            throws ValidationError {
        final Map<Long, StorageSystem> systemMappings = this.systemUtil.getStorageSystems();
        final String datasetSubString = datasetStr.equals("All") ? "" : datasetStr;
        final Page<Dataset> pagedDatasets = this.pageDatasetRepository
                .findByIsActiveYNAndStorageDataSetNameContaining(ActiveEnumeration.NO.getFlag(), datasetSubString,
                        pageable);

        final List<Dataset> listDatasets = pagedDatasets.getContent();

        for (final Dataset dataset : listDatasets) {
            final ObjectSchemaMap schemaMap = this.schemaMapUtil
                    .validateObjectSchemaMapId(dataset.getObjectSchemaMapId());
            final long storageSystemId = schemaMap.getStorageSystemId();
            final StorageSystem storageSystem = systemMappings.get(storageSystemId);
            final StorageType storageType = this.systemUtil.getStorageType(storageSystemId);
            this.populateAvailableDataset(storageSystem, dataset, storageType);
            dataset.setStorageSystemId(storageSystemId);
        }
        return pagedDatasets;

    }

    public Page<Dataset> getAllPendingDatasetsForSystem(final String datasetStr, final String storageSystemName,
            final Pageable pageable) throws ValidationError {
        final String datasetSubString = datasetStr.equals("All") ? "" : datasetStr;
        final StorageSystem system = this.storageSystemRepository.findByStorageSystemName(storageSystemName);
        if (system == null) {
            return new PageImpl<Dataset>(new ArrayList<Dataset>(), pageable, 0);
        }
        final StorageType storageType = this.storageTypeUtil.validateStorageTypeId(system.getStorageSystemId());
        final List<DatasetStorageSystem> datasetsForStorageSystem = this.datasetSystemRepository
                .findByStorageSystemIdAndIsActiveYN(system.getStorageSystemId(),
                        ActiveEnumeration.YES.getFlag());

        final List<Long> datasetIds = new ArrayList<Long>();
        datasetsForStorageSystem.forEach(dss -> {
            datasetIds.add(dss.getStorageDataSetId());
        });

        final Page<Dataset> pagedDatasets = this.pageDatasetRepository
                .findByStorageDataSetIdInAndStorageDataSetNameContaining(datasetIds, datasetSubString, pageable);
        pagedDatasets.getContent().stream().forEach(dataset -> {
            this.populatePendingDataset(system, dataset, storageType);
        });
        return pagedDatasets;

    }

    public Page<Dataset> getAllPendingDatasetsForSystemIds(final String datasetStr, final List<Long> storageSystemIds,
            final Pageable pageable, final StorageType storageType) {

        final String datasetSubString = datasetStr.equals("All") ? "" : datasetStr;
        final Map<Long, DatasetStorageSystem> datasetsForStorageSystems = this
                .getDatasetStorageSystemMappings(storageSystemIds, ActiveEnumeration.YES.getFlag());
        final Map<Long, StorageSystem> systemMap = this.systemUtil.getStorageSystems();
        final List<Long> datasetIds = datasetsForStorageSystems.keySet().stream()
                .collect(Collectors.toList());
        final Page<Dataset> pagedDatasets = this.pageDatasetRepository
                .findByStorageDataSetIdInAndStorageDataSetNameContaining(datasetIds, datasetSubString, pageable);
        pagedDatasets.getContent().stream().forEach(dataset -> {
            final DatasetStorageSystem datasetSystem = datasetsForStorageSystems.get(dataset.getStorageDataSetId());
            final StorageSystem storageSystem = systemMap.get(datasetSystem.getStorageSystemId());
            this.populatePendingDataset(storageSystem, dataset, storageType);
        });
        return pagedDatasets;
    }

    private void populateAvailableDataset(final StorageSystem storageSystem, final Dataset dataset,
            final StorageType storageType) {
        final Map<Long, Zone> zones = this.zoneUtil.getZones();
        dataset.setStorageSystemName(storageSystem.getStorageSystemName());
        final long objectSchemaMapId = dataset.getObjectSchemaMapId();

        final boolean isAttributesPresent = this.validateAttributes(objectSchemaMapId,
                storageSystem.getStorageSystemId(),
                storageType.getStorageTypeId());
        dataset.setAttributesPresent(isAttributesPresent);
        dataset.setIsReadCompatible(storageSystem.getIsReadCompatible());
        dataset.setZoneName(zones.get(storageSystem.getZoneId()).getZoneName());
    }

    private void populatePendingDataset(final StorageSystem system, final Dataset dataset,
            final StorageType storageType) {
        dataset.setStorageSystemName(system.getStorageSystemName());
        dataset.setIsReadCompatible(system.getIsReadCompatible());
        final long objectSchemaMapId = dataset.getObjectSchemaMapId();
        final boolean isAttributesPresent = this.validateAttributes(objectSchemaMapId, system.getStorageSystemId(),
                storageType.getStorageTypeId());
        dataset.setAttributesPresent(isAttributesPresent);
    }

    private void closeConnection(final Connection connection) throws SQLException {
        connection.close();
    }

    public List<List<String>> getMaskedData(final List<List<String>> output, final List<Schema> columns) {
        this.restrictedColumns.add("restricted_columns_class2");
        this.restrictedColumns.add("restricted_columns_class3_1");
        this.restrictedColumns.add("restricted_columns_class3_2");
        final List<List<String>> maskedOutput = new ArrayList<List<String>>();
        final Map<Integer, String> schemaIndex = new HashMap<Integer, String>();
        int i = 0;
        for (final Schema column : columns) {
            schemaIndex.put(i, column.getColumnClass());
            i++;
        }
        maskedOutput.add(output.get(0));
        for (int j = 1; j < output.size(); j++) {
            final List<String> row = output.get(j);
            final List<String> maskedRow = new ArrayList<String>();
            for (int k = 0; k < row.size(); k++) {
                final String columnClass = schemaIndex.get(k);
                if (this.restrictedColumns.contains(columnClass)) {
                    maskedRow.add("XXXX");
                }
                else {
                    maskedRow.add(row.get(k));
                }
            }
            maskedOutput.add(maskedRow);
        }
        return maskedOutput;
    }

    public void upsertDataset(final String esDatasetIndex, final String esType, final Dataset tempDataset,
            final ElasticsearchTemplate esTemplate)
            throws ValidationError, IOException, InterruptedException, ExecutionException {
        final List<ElasticSearchDataset> esDatasets = this.esDatasetRepository
                .findByStorageDataSetId(tempDataset.getStorageDataSetId());

        if (esDatasets == null || esDatasets.isEmpty()) {
            this.save(esDatasetIndex, esType, tempDataset);
            esTemplate.refresh(esDatasetIndex);
        }
        else {
            this.update(esDatasetIndex, esType, esDatasets.get(0), tempDataset);
            esTemplate.refresh(esDatasetIndex);
        }
    }

    public void save(final String indexName, final String typeName, final Dataset tempDataset) throws ValidationError {

        final Gson gson = new Gson();

        final StorageSystem storageSystem = this.systemUtil.validateStorageSystem(tempDataset.getStorageSystemId());
        final ObjectSchemaMap schemaMap = this.schemaMapUtil
                .validateObjectSchemaMapId(tempDataset.getObjectSchemaMapId());
        final List<Schema> objectSchema = gson.fromJson(schemaMap.getObjectSchemaInString(),
                new TypeToken<List<Schema>>() {
                }.getType());

        // get All the Object Attribute Values assigned to dataset
        final List<ObjectAttributeValue> attributeValues = schemaMap != null ? this.objectSchemaAttributeRepository
                .findByObjectIdAndIsActiveYN(schemaMap.getObjectId(), ActiveEnumeration.YES.getFlag()) : null;

        // get All the custom object Attribute Values to a dataset
        final List<ObjectAttributeKeyValue> customObjectAttributes = this.sackvr
                .findByObjectId(tempDataset.getObjectSchemaMapId());

        // get All the tags assigned to the dataset from MySQL
        final List<Tag> tagMaps = this.tagUtil.getTagsByDatasetId(tempDataset.getStorageDataSetId());

        // get All the owner assigned to the dataset from MySQL
        final List<DatasetOwnershipMap> ownerMaps = this.ownershipUtil
                .getOwnersByDatasetId((tempDataset.getStorageDataSetId()));

        // get All Descriptions
        final List<SourceProviderDatasetMap> datasetDescriptionMap = this.getObjectColumnDescriptions(tempDataset,
                storageSystem);

        final ElasticSearchDataset esDataset = new ElasticSearchDataset(tempDataset.getStorageDataSetId(),
                tempDataset.getStorageDataSetName(), tempDataset.getStorageDataSetAliasName(),
                tempDataset.getStorageDatabaseName(), tempDataset.getStorageDataSetDescription(),
                tempDataset.getIsActiveYN(), tempDataset.getUserId(), tempDataset.getCreatedUser(),
                tempDataset.getCreatedTimestamp(), tempDataset.getUpdatedUser(), tempDataset.getUpdatedTimestamp(),
                tempDataset.getIsAutoRegistered(), tempDataset.getObjectSchemaMapId(), tempDataset.getStorageSystemId(),
                storageSystem.getZoneId(), storageSystem.getIsReadCompatible(),
                schemaMap.getContainerName(), schemaMap.getObjectName(), schemaMap.getIsSelfDiscovered(),
                schemaMap.getIsRegistered(), schemaMap.getCreatedUserOnStore(), schemaMap.getCreatedTimestampOnStore(),
                objectSchema, attributeValues, tagMaps, ownerMaps, datasetDescriptionMap, customObjectAttributes);

        final IndexResponse response = this.client.prepareIndex(indexName, typeName, esDataset.get_id())
                .setSource(gson.toJson(esDataset), XContentType.JSON)
                .get();
    }

    public String update(final String indexName, final String typeName,
            final ElasticSearchDataset esDataset, final Dataset dataset)
            throws IOException, InterruptedException, ExecutionException, ValidationError {

        final Gson gson = new Gson();
        final ObjectSchemaMap schemaMap = this.schemaMapUtil
                .validateObjectSchemaMapId(dataset.getObjectSchemaMapId());

        final List<Schema> objectSchema = gson.fromJson(schemaMap.getObjectSchemaInString(),
                new TypeToken<List<Schema>>() {
                }.getType());
        dataset.setObjectSchema(objectSchema);

        // get All the Object Attribute Values assigned to dataset
        final List<ObjectAttributeValue> attributeValues = schemaMap != null ? this.objectSchemaAttributeRepository
                .findByObjectIdAndIsActiveYN(schemaMap.getObjectId(), ActiveEnumeration.YES.getFlag()) : null;
        dataset.setObjectAttributeValues(attributeValues);

        // get All the custom object Attribute Values to a dataset
        final List<ObjectAttributeKeyValue> customObjectAttributes = this.sackvr
                .findByObjectId(dataset.getObjectSchemaMapId());
        dataset.setCustomAttributeValues(customObjectAttributes);

        // get All the tags assigned to the dataset from MySQL
        final List<Tag> tagMaps = this.tagUtil.getTagsByDatasetId(dataset.getStorageDataSetId());
        dataset.setTags(tagMaps);

        // get All the owner assigned to the dataset from MySQL
        final List<DatasetOwnershipMap> ownerMaps = this.ownershipUtil
                .getOwnersByDatasetId((dataset.getStorageDataSetId()));
        dataset.setOwnerships(ownerMaps);

        // get All Descriptions
        final StorageSystem storageSystem = this.systemUtil.validateStorageSystem(dataset.getStorageSystemId());
        final List<SourceProviderDatasetMap> datasetDescriptionMap = this.getObjectColumnDescriptions(dataset,
                storageSystem);
        dataset.setObjectDescriptions(datasetDescriptionMap);

        final UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index(indexName).type(typeName)
                .id(esDataset.get_id())
                .doc(gson.toJson(dataset), XContentType.JSON);

        final UpdateResponse updateResponse = this.client.update(updateRequest).get();
        return updateResponse.status().toString();
    }

    public List<SourceProviderDatasetMap> getObjectColumnDescriptions(final Dataset tempDataset,
            final StorageSystem storageSystem) {
        final long datasetId = tempDataset.getStorageDataSetId();
        final List<SourceProviderDatasetMap> datasetDescriptionMap = new ArrayList<SourceProviderDatasetMap>();
        if (storageSystem.getStorageSystemName().contains("Teradata")) {
            // retrieve dataset description from teradata schema_dataset_map table in MySQL
            final List<SchemaDatasetMap> datasetDescriptionMapTeradata = this.schemaDatasetMapRepository
                    .findByStorageDatasetId(datasetId);
            datasetDescriptionMapTeradata.forEach(description -> {
                final List<SchemaDatasetColumnMap> columnMap = this.schemaDatasetColumnMapRepository
                        .findBySchemaDatasetMapId(description.getSchemaDatasetMapId());
                final SourceProviderDatasetMap descriptionProviderMap = new SourceProviderDatasetMap(
                        datasetId, description.getProviderId(), description.getObjectComment(),
                        description.getCreatedUser(), description.getCreatedTimestamp(), description.getUpdatedUser(),
                        description.getUpdatedTimestamp(), columnMap, null);
                datasetDescriptionMap.add(descriptionProviderMap);
            });
        }
        else {
            // retrieve desc from ataset_description table in MySQL - Applicable for datasets which are non teradata
            final List<DatasetDescriptionMap> datasetDescriptionMapMisc = this.datasetDescriptionMapRepository
                    .findByStorageDatasetId(tempDataset.getStorageDataSetId());
            datasetDescriptionMapMisc.forEach(description -> {
                final List<DatasetColumnDescriptionMap> columnMap = this.datasetColumnDescriptionMapRepository
                        .findByBodhiDatasetMapId(description.getBodhiDatasetMapId());
                final SourceProviderDatasetMap descriptionProviderMap = new SourceProviderDatasetMap(
                        datasetId, description.getProviderId(), description.getObjectComment(),
                        description.getCreatedUser(), description.getCreatedTimestamp(), description.getUpdatedUser(),
                        description.getUpdatedTimestamp(), null, columnMap);
                datasetDescriptionMap.add(descriptionProviderMap);
            });
        }

        return datasetDescriptionMap;

    }

    // public SearchQuery buildTypeQuery(final String datasetSubString) {
    // BoolQueryBuilder qb = QueryBuilders.boolQuery();
    // QueryBuilder storageBuilder = null;
    // for (final String column : SearchFieldEnumeration.TYPE.getKey().split(",")) {
    // final QueryBuilder builder = QueryBuilders.regexpQuery(column, ".*" + datasetSubString + ".*");
    // qb = qb.should(builder);
    // storageBuilder = qb;
    // }
    // final SearchQuery searchQuery = new NativeSearchQueryBuilder()
    // .withFilter(storageBuilder).build();
    // return searchQuery;
    // }
    //
    // public SearchQuery buildSystemQuery(final String datasetSubString) {
    // BoolQueryBuilder qb = QueryBuilders.boolQuery();
    // QueryBuilder storageBuilder = null;
    // for (final String column : SearchFieldEnumeration.SYSTEM.getKey().split(",")) {
    // final QueryBuilder builder = QueryBuilders.regexpQuery(column, ".*" + datasetSubString + ".*");
    // qb = qb.should(builder);
    // storageBuilder = qb;
    // }
    // final SearchQuery searchQuery = new NativeSearchQueryBuilder()
    // .withFilter(storageBuilder).build();
    // return searchQuery;
    // }
    //
    // public SearchQuery buildZoneQuery(final String datasetSubString) {
    // BoolQueryBuilder qb = QueryBuilders.boolQuery();
    // QueryBuilder zoneBuilder = null;
    // for (final String column : SearchFieldEnumeration.ZONE.getKey().split(",")) {
    // final QueryBuilder builder = QueryBuilders.regexpQuery(column, ".*" + datasetSubString + ".*");
    // qb = qb.should(builder);
    // zoneBuilder = qb;
    // }
    // final SearchQuery searchQuery = new NativeSearchQueryBuilder()
    // .withFilter(zoneBuilder).build();
    // return searchQuery;
    // }

    // public SearchQuery buildDatasetQuery(final String datasetSubString, final Pageable pageable,
    // final Map<String, SearchFilterAttribute> searchFilterMap) {
    // final BoolQueryBuilder qb = QueryBuilders.boolQuery();
    // final BoolQueryBuilder filterQb = QueryBuilders.boolQuery();
    // final List<Long> systemIds = searchFilterMap.get(SearchFilterEnumeration.SYSTEM.getKey()).getIds();
    // final boolean systemFilter = searchFilterMap.get(SearchFilterEnumeration.SYSTEM.getKey()).isFilter();
    // final boolean zoneFilter = searchFilterMap.get(SearchFilterEnumeration.ZONE.getKey()).isFilter();
    // final boolean typeFilter = searchFilterMap.get(SearchFilterEnumeration.TYPE.getKey()).isFilter();
    // final List<Long> zoneIds = searchFilterMap.get(SearchFilterEnumeration.ZONE.getKey()).getIds();
    //
    // BoolQueryBuilder systemQb = QueryBuilders.boolQuery();
    // for (final long systemId : systemIds) {
    // final QueryBuilder queryBuilder = QueryBuilders.termQuery(SearchFieldEnumeration.SYSTEMID.getKey(),
    // systemId);
    // systemQb = systemQb.should(queryBuilder);
    // }
    //
    // BoolQueryBuilder zoneQb = QueryBuilders.boolQuery();
    // for (final long zoneId : zoneIds) {
    // final QueryBuilder queryBuilder = QueryBuilders.termQuery(SearchFieldEnumeration.ZONEID.getKey(),
    // zoneId);
    // zoneQb = zoneQb.should(queryBuilder);
    // }
    //
    // final QueryBuilder filterBuilder = filterQb.must(systemQb).must(zoneQb);
    //
    // BoolQueryBuilder datasetBuilder = QueryBuilders.boolQuery();
    // for (final String column : SearchFieldEnumeration.DATASET.getKey().split(",")) {
    // final QueryBuilder builder = QueryBuilders.regexpQuery(column, ".*" + datasetSubString + ".*");
    // datasetBuilder = datasetBuilder.should(builder);
    // }
    //
    // final QueryBuilder activeBuilder = QueryBuilders.termQuery("isActiveYN.keyword",
    // ActiveEnumeration.YES.getFlag());
    //
    // if (systemFilter || zoneFilter || typeFilter) {
    // final SearchQuery searchQuery = new NativeSearchQueryBuilder()
    // .withFilter(qb.must(activeBuilder).must(filterBuilder).must(datasetBuilder))
    // .withPageable(pageable)
    // .build();
    // return searchQuery;
    // }
    // else {
    // final SearchQuery searchQuery = new NativeSearchQueryBuilder()
    // .withFilter(qb.must(activeBuilder).should(filterBuilder).must(datasetBuilder))
    // .withPageable(pageable)
    // .build();
    // return searchQuery;
    // }
    //
    // }

    // public Page<ElasticSearchDataset> buildDatasetQuery(final String searchString, final Pageable pageable,
    // final List<Long> systemIds) {
    // return this.esDatasetRepository.findByStorageSystemIdIn(systemIds, pageable);
    // }

    public void populateZoneNameAndSystemName(final Map<Long, StorageSystem> systemMappings,
            final Map<Long, StorageType> typeMappings, final ElasticSearchDataset dataset) {

        final StorageSystem system = systemMappings.get(dataset.getStorageSystemId());
        dataset.setStorageSystemName(system.getStorageSystemName());
        final List<ElasticSearchStorageType> storageTypes = this.esTypeRepository
                .findByStorageTypeId(system.getStorageTypeId());

        final String typeName = storageTypes == null || storageTypes.size() == 0
                ? typeMappings.get(system.getStorageTypeId()).getStorageTypeName()
                : storageTypes.get(0).getStorageTypeName();
        final ElasticSearchZone zone = this.esZoneRepository.findByZoneId(dataset.getZoneId()).get(0);

        final Dataset tempDataset = new Dataset(dataset.getStorageDataSetName(), system.getStorageSystemId(),
                dataset.getStorageDataSetAliasName(), dataset.getContainerName(),
                dataset.getStorageDatabaseName(), dataset.getStorageDataSetDescription(),
                dataset.getIsActiveYN(), dataset.getCreatedUser(), dataset.getCreatedTimestamp(),
                dataset.getUpdatedUser(), dataset.getUpdatedTimestamp(), dataset.getUserId(),
                dataset.getIsAutoRegistered());
        this.setAccessControlForDataset(typeName, system,
                dataset.getObjectSchemaMapId(), tempDataset);
        dataset.setIsAccessControlled(tempDataset.getIsAccessControlled());
        dataset.setZoneName(zone.getZoneName());
    }

    public Page<ElasticSearchDataset> getAllDatasetsByEsType(final String dataSetSubString,
            final String searchType, final String storageSystems, final String containerName, final Pageable pageable) {

        Page<ElasticSearchDataset> esDatasets = null;
        final SearchFieldEnumeration searchTypeEnum = SearchFieldEnumeration.valueOf(searchType);
        final List<Long> systemIds = new ArrayList<Long>();
        final Map<String, StorageSystem> systemMap = this.systemUtil.getStorageSystemsByName();
        final Map<Long, Zone> zoneMap = this.zoneUtil.getZoneMappings();
        final Map<Long, Entity> entityMap = this.entityUtil.getEntities();
        final Map<Long, StorageType> typeMap = this.storageTypeUtil.getStorageTypes();
        if (storageSystems.equals("All")) {
            this.storageSystemRepository.findAll().forEach(system -> systemIds.add(system.getStorageSystemId()));
        }
        else {

            final List<String> storageSystemList = Arrays.asList(storageSystems.split(","));
            systemIds.addAll(storageSystemList.stream()
                    .map(systemName -> systemMap.get(systemName) != null
                            ? systemMap.get(systemName).getStorageSystemId()
                            : 0)
                    .filter(systemId -> systemId != 0)
                    .collect(Collectors.toList()));
        }

        if (systemIds.size() == 0) {
            final List<ElasticSearchDataset> emptyList = new ArrayList<ElasticSearchDataset>();
            final Page<ElasticSearchDataset> page = new PageImpl<>(emptyList);
            return page;
        }

        if (!dataSetSubString.equals("All")) {
            switch (searchTypeEnum) {
                case DATASET: {
                    esDatasets = containerName.equals("All") ? this.esDatasetRepository
                            .findByStorageDataSetNameContainingAndStorageSystemIdInAndIsActiveYN(dataSetSubString,
                                    systemIds, ActiveEnumeration.YES.getFlag(), pageable)
                            : this.esDatasetRepository
                                    .findByStorageDataSetNameContainingAndStorageSystemIdInAndIsActiveYNAndContainerName(
                                            dataSetSubString, systemIds, ActiveEnumeration.YES.getFlag(), containerName,
                                            pageable);
                    break;
                }

                case OBJECT_ATTRIBUTE: {
                    esDatasets = containerName.equals("All") ? this.esDatasetRepository
                            .findByIsActiveYNAndStorageSystemIdInAndObjectAttributeValues_ObjectAttributeValueContainingOrIsActiveYNAndStorageSystemIdInAndCustomAttributeValues_ObjectAttributeValueContaining(
                                    ActiveEnumeration.YES.getFlag(), systemIds, dataSetSubString,
                                    ActiveEnumeration.YES.getFlag(), systemIds, dataSetSubString,
                                    pageable)
                            : this.esDatasetRepository
                                    .findByIsActiveYNAndStorageSystemIdInAndContainerNameAndObjectAttributeValues_ObjectAttributeValueContainingOrIsActiveYNAndStorageSystemIdInAndContainerNameAndCustomAttributeValues_ObjectAttributeValueContaining(
                                            ActiveEnumeration.YES.getFlag(), systemIds, containerName, dataSetSubString,
                                            ActiveEnumeration.YES.getFlag(), systemIds, containerName, dataSetSubString,
                                            pageable);
                    break;
                }

                case DATASET_DESCRIPTION: {
                    esDatasets = containerName.equals("All") ? this.esDatasetRepository
                            .findByObjectDescriptions_ObjectCommentContainingAndIsActiveYNAndStorageSystemIdIn(
                                    dataSetSubString, ActiveEnumeration.YES.getFlag(), systemIds, pageable)
                            : this.esDatasetRepository
                                    .findByObjectDescriptions_ObjectCommentContainingAndIsActiveYNAndContainerNameAndStorageSystemIdIn(
                                            dataSetSubString, ActiveEnumeration.YES.getFlag(), containerName, systemIds,
                                            pageable);
                    break;
                }

                case COLUMN_DESCRIPTION: {
                    esDatasets = containerName.equals("All") ? this.esDatasetRepository
                            .findByObjectDescriptions_SchemaColumnDescriptions_ColumnCommentContainingOrObjectDescriptions_MiscColumnDescriptions_ColumnCommentContainingAndIsActiveYNAndStorageSystemIdIn(
                                    dataSetSubString, ActiveEnumeration.YES.getFlag(), systemIds, pageable)
                            : this.esDatasetRepository
                                    .findByObjectDescriptions_SchemaColumnDescriptions_ColumnCommentContainingOrObjectDescriptions_MiscColumnDescriptions_ColumnCommentContainingAndIsActiveYNAndStorageSystemIdInAndContainerName(
                                            dataSetSubString, ActiveEnumeration.YES.getFlag(), systemIds, containerName,
                                            pageable);
                    break;
                }

                case OBJECT_SCHEMA: {
                    esDatasets = containerName.equals("All") ? this.esDatasetRepository
                            .findByObjectSchema_ColumnNameContainingAndIsActiveYNAndStorageSystemIdIn(dataSetSubString,
                                    ActiveEnumeration.YES.getFlag(), systemIds, pageable)
                            : this.esDatasetRepository
                                    .findByObjectSchema_ColumnNameContainingAndIsActiveYNAndStorageSystemIdInAndContainerName(
                                            dataSetSubString, ActiveEnumeration.YES.getFlag(), systemIds, containerName,
                                            pageable);
                    break;
                }

                case TAG: {
                    esDatasets = containerName.equals("All")
                            ? this.esDatasetRepository.findByTags_TagNameContainingAndIsActiveYNAndStorageSystemIdIn(
                                    dataSetSubString, ActiveEnumeration.YES.getFlag(), systemIds, pageable)
                            : this.esDatasetRepository
                                    .findByTags_TagNameContainingAndIsActiveYNAndStorageSystemIdInAndContainerName(
                                            dataSetSubString, ActiveEnumeration.YES.getFlag(), systemIds, containerName,
                                            pageable);
                    break;
                }

                default: {
                    esDatasets = containerName.equals("All") ? this.esDatasetRepository
                            .findByStorageDataSetNameContainingAndStorageSystemIdInAndIsActiveYN(dataSetSubString,
                                    systemIds, ActiveEnumeration.YES.getFlag(), pageable)
                            : this.esDatasetRepository
                                    .findByStorageDataSetNameContainingAndStorageSystemIdInAndIsActiveYNAndContainerName(
                                            dataSetSubString, systemIds, ActiveEnumeration.YES.getFlag(), containerName,
                                            pageable);
                }
            }
        }
        else {
            esDatasets = containerName.equals("All")
                    ? this.esDatasetRepository.findByStorageSystemIdInAndIsActiveYN(systemIds,
                            ActiveEnumeration.YES.getFlag(), pageable)
                    : this.esDatasetRepository.findByStorageSystemIdInAndIsActiveYNAndContainerName(systemIds,
                            ActiveEnumeration.YES.getFlag(), containerName, pageable);
        }

        esDatasets.getContent()
                .forEach(esDataset -> {
                    final String storageSystemName = systemMap.entrySet().stream()
                            .filter(map -> esDataset.getStorageSystemId() == map.getValue().getStorageSystemId())
                            .findFirst().get().getKey();
                    esDataset.setStorageSystemName(storageSystemName);
                    final StorageSystem storageSystem = systemMap.get(storageSystemName);
                    esDataset.setZoneName(zoneMap.get(storageSystem.getZoneId()).getZoneName());
                    esDataset.setEntityName(entityMap.get(storageSystem.getEntityId()).getEntityName());
                    final Dataset dataset = new Dataset(esDataset.getStorageDataSetName(),
                            esDataset.getStorageDataSetAliasName(), esDataset.getContainerName(),
                            esDataset.getObjectName(), esDataset.getStorageDataSetDescription(),
                            esDataset.getIsActiveYN(), esDataset.getCreatedUser(), esDataset.getCreatedTimestamp(),
                            esDataset.getUpdatedUser(), esDataset.getUpdatedTimestamp(), esDataset.getUserId(),
                            esDataset.getIsRegistered(), esDataset.getObjectSchemaMapId(),
                            esDataset.getStorageSystemId());
                    this.setAccessControlForDataset(typeMap.get(storageSystem.getStorageTypeId()).getStorageTypeName(),
                            storageSystem, esDataset.getObjectSchemaMapId(), dataset);
                    esDataset.setIsAccessControlled(dataset.getIsAccessControlled());
                });
        return esDatasets;
    }

    public void validateDataset(final List<Long> datasetsIds) throws ValidationError {

        for (final Long datasetId : datasetsIds) {
            final Dataset dataset = this.dataSetRepository.findByStorageDataSetId(datasetId);
            if (dataset == null) {
                final ValidationError v = new ValidationError();
                v.setErrorCode(HttpStatus.BAD_REQUEST);
                v.setErrorDescription("Dataset ID is not valid");
                throw v;
            }
        }
    }
}
