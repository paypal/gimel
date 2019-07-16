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
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import javax.validation.ConstraintViolationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.paypal.udc.dao.cluster.ClusterRepository;
import com.paypal.udc.dao.dataset.DatasetChangeLogRepository;
import com.paypal.udc.dao.dataset.DatasetRepository;
import com.paypal.udc.dao.dataset.DatasetStorageSystemRepository;
import com.paypal.udc.dao.objectschema.ObjectSchemaAttributeCustomKeyValueRepository;
import com.paypal.udc.dao.objectschema.ObjectSchemaAttributeValueRepository;
import com.paypal.udc.dao.objectschema.ObjectSchemaMapRepository;
import com.paypal.udc.dao.ownership.DatasetOwnershipMapRepository;
import com.paypal.udc.dao.storagesystem.StorageSystemAttributeValueRepository;
import com.paypal.udc.dao.storagesystem.StorageSystemRepository;
import com.paypal.udc.dao.storagetype.StorageTypeRepository;
import com.paypal.udc.dao.zone.ZoneRepository;
import com.paypal.udc.entity.cluster.Cluster;
import com.paypal.udc.entity.cluster.ClusterAttributeValue;
import com.paypal.udc.entity.dataset.CumulativeDataset;
import com.paypal.udc.entity.dataset.Dataset;
import com.paypal.udc.entity.dataset.DatasetChangeLog;
import com.paypal.udc.entity.dataset.DatasetStorageSystem;
import com.paypal.udc.entity.dataset.DatasetWithAttributes;
import com.paypal.udc.entity.dataset.ElasticSearchDataset;
import com.paypal.udc.entity.objectschema.ObjectAttributeKeyValue;
import com.paypal.udc.entity.objectschema.ObjectAttributeValue;
import com.paypal.udc.entity.objectschema.ObjectSchemaMap;
import com.paypal.udc.entity.objectschema.Schema;
import com.paypal.udc.entity.ownership.DatasetOwnershipMap;
import com.paypal.udc.entity.storagesystem.StorageSystem;
import com.paypal.udc.entity.storagesystem.StorageSystemAttributeValue;
import com.paypal.udc.entity.storagetype.StorageType;
import com.paypal.udc.entity.storagetype.StorageTypeAttributeKey;
import com.paypal.udc.entity.zone.Zone;
import com.paypal.udc.exception.ValidationError;
import com.paypal.udc.service.IDatasetService;
import com.paypal.udc.util.ClusterUtil;
import com.paypal.udc.util.DatasetDescriptionUtil;
import com.paypal.udc.util.DatasetUtil;
import com.paypal.udc.util.ObjectSchemaMapUtil;
import com.paypal.udc.util.SchemaDatasetUtil;
import com.paypal.udc.util.StorageSystemUtil;
import com.paypal.udc.util.StorageTypeUtil;
import com.paypal.udc.util.ZoneUtil;
import com.paypal.udc.util.enumeration.ActiveEnumeration;
import com.paypal.udc.util.enumeration.ClusterAttributeKeyTypeEnumeration;
import com.paypal.udc.util.enumeration.TimelineEnumeration;
import com.paypal.udc.validator.dataset.DatasetAliasValidator;
import com.paypal.udc.validator.dataset.DatasetDescValidator;
import com.paypal.udc.validator.dataset.DatasetNameValidator;


@Service
@Transactional(propagation = Propagation.SUPPORTS, readOnly = true)
public class DatasetService implements IDatasetService {

    final static Logger logger = LoggerFactory.getLogger(DatasetService.class);
    final static String databaseName = "pcatalog";

    @Autowired
    private StorageSystemUtil systemUtil;

    @Autowired
    private DatasetRepository dataSetRepository;

    @Autowired
    private ClusterUtil clusterUtil;

    @Autowired
    private StorageSystemAttributeValueRepository systemAttributeValueRepository;

    @Autowired
    private DatasetStorageSystemRepository datasetStorageSystemRepository;

    @Autowired
    private StorageSystemRepository storageSystemRepository;

    @Autowired
    private ZoneRepository zoneRepository;

    @Autowired
    private DatasetUtil dataSetUtil;

    @Autowired
    private StorageTypeUtil storageTypeUtil;

    @Autowired
    private StorageSystemUtil storageSystemUtil;

    @Autowired
    private ZoneUtil zoneUtil;

    @Autowired
    private StorageTypeRepository storageTypeRepository;

    @Autowired
    private DatasetNameValidator s1;

    @Autowired
    private DatasetDescValidator s2;

    @Autowired
    private DatasetAliasValidator s4;

    @Value("${elasticsearch.dataset.name}")
    private String esDatasetIndex;

    @Value("${elasticsearch.type.name}")
    private String esType;

    @Value("${udc.es.write.enabled}")
    private String isEsWriteEnabled;

    @Autowired
    private ElasticsearchTemplate esTemplate;

    @Autowired
    private DatasetChangeLogRepository changeLogRepository;

    @Autowired
    private ObjectSchemaMapRepository objectSchemaMapRepository;

    @Autowired
    private ObjectSchemaAttributeValueRepository objectSchemaAttributeRepository;

    @Autowired
    private ObjectSchemaAttributeCustomKeyValueRepository sackvr;

    @Autowired
    private ClusterRepository clusterRepository;

    @Autowired
    private ObjectSchemaMapUtil schemaMapUtil;

    @Autowired
    private SchemaDatasetUtil schemaDatasetUtil;

    @Autowired
    private DatasetDescriptionUtil bodhiDatasetUtil;

    @Autowired
    private DatasetOwnershipMapRepository ownershipRepository;

    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss");

    final List<StorageSystem> typeZoneList = new ArrayList<StorageSystem>();

    final List<StorageSystem> typeZoneSystemList = new ArrayList<StorageSystem>();

    final List<StorageSystem> zoneStorageSystemList = new ArrayList<StorageSystem>();

    @Override
    public List<CumulativeDataset> getAllDetailedDatasets(final String dataSetSubString) {
        final Map<Long, StorageSystem> storageSystemMappings = this.systemUtil.getStorageSystems();
        final Map<Long, Zone> zones = this.zoneUtil.getZones();
        final Gson gson = new Gson();
        final List<Object[]> datasetStorageTypes = this.dataSetRepository
                .getAllDatasetsWithType(ActiveEnumeration.YES.getFlag(), dataSetSubString);
        final List<CumulativeDataset> cummulativeDatasets = new ArrayList<CumulativeDataset>();
        datasetStorageTypes.stream().forEach(datasetStorageType -> {
            final List<Dataset> datasets = gson.fromJson(String.valueOf(datasetStorageType[2]),
                    new TypeToken<List<Dataset>>() {
                    }.getType());
            datasets.forEach(dataset -> {
                final long storageSystemId = dataset.getStorageSystemId();
                final StorageSystem storageSystem = storageSystemMappings.get(storageSystemId);
                final long zoneId = storageSystem.getZoneId();
                dataset.setStorageSystemName(storageSystem.getStorageSystemName());
                dataset.setIsReadCompatible(storageSystem.getIsReadCompatible());
                dataset.setZoneName(zones.get(zoneId).getZoneName());
            });
            final CumulativeDataset cumDataset = new CumulativeDataset(
                    Long.valueOf(String.valueOf(datasetStorageType[0])),
                    String.valueOf(datasetStorageType[1]), datasets);
            cummulativeDatasets.add(cumDataset);
        });
        return cummulativeDatasets;
    }

    @Override
    public List<CumulativeDataset> getAllDatasets(final String datasetSubstring) {

        final Gson gson = new Gson();
        final List<Object[]> datasetStorageTypes = this.dataSetRepository
                .getAllDatasetsWithType(ActiveEnumeration.YES.getFlag(), datasetSubstring);

        return datasetStorageTypes.stream()
                .map(datasetStorageType -> new CumulativeDataset(Long.valueOf(String.valueOf(datasetStorageType[0])),
                        String.valueOf(datasetStorageType[1]),
                        gson.fromJson(String.valueOf(datasetStorageType[2]), new TypeToken<List<Dataset>>() {
                        }.getType())))
                .collect(Collectors.toList());
    }

    @Override
    public Page<ElasticSearchDataset> getAllDatasetsFromESByType(final String dataSetSubString, final Pageable pageable,
            final String searchType, final String storageSystems, final String containerName) throws ValidationError {
        return this.dataSetUtil.getAllDatasetsByEsType(dataSetSubString, searchType, storageSystems, containerName,
                pageable);
    }

    // @Override
    // public Page<ElasticSearchDataset> getAllDatasetsFromES(final String datasetSubString, final Pageable pageable,
    // final String storageTypeName, final String storageSystemName, final String zoneName)
    // throws ValidationError {
    //
    // List<Long> typeIds = null;
    // List<Long> systemIds = null;
    // List<Long> zoneIds = null;
    //
    // final String searchString = datasetSubString.equals("All") ? "" : datasetSubString;
    // final Map<Long, StorageSystem> systemMappings = this.systemUtil.getStorageSystems();
    // final Map<Long, StorageType> typeMappings = this.storageTypeUtil.getStorageTypes();
    // final Map<String, SearchFilterAttribute> searchFilterMap = new HashMap<String, SearchFilterAttribute>();
    //
    // if (storageTypeName.equals("All")) {
    // typeIds = this.storageTypeUtil.getStorageTypes().keySet().stream().collect(Collectors.toList());
    // final SearchFilterAttribute typeFilter = new SearchFilterAttribute(typeIds, false,
    // SearchFilterEnumeration.TYPE.getKey());
    // searchFilterMap.put(SearchFilterEnumeration.TYPE.getKey(), typeFilter);
    // }
    // else {
    // final StorageType storageType = this.storageTypeRepository.findByStorageTypeName(storageTypeName);
    // if (storageType == null) {
    // final ValidationError verror = new ValidationError();
    // verror.setErrorCode(HttpStatus.BAD_GATEWAY);
    // verror.setErrorDescription("Invalid Storage Type Name");
    // throw verror;
    // }
    // typeIds = new ArrayList<Long>();
    // typeIds.add(storageType.getStorageTypeId());
    // final SearchFilterAttribute typeFilter = new SearchFilterAttribute(typeIds, true,
    // SearchFilterEnumeration.TYPE.getKey());
    // searchFilterMap.put(SearchFilterEnumeration.TYPE.getKey(), typeFilter);
    // }
    //
    // if (storageSystemName.equals("All")) {
    // systemIds = this.storageSystemRepository.findByStorageTypeIdIn(typeIds).stream()
    // .map(system -> system.getStorageSystemId()).distinct().collect(Collectors.toList());
    // final SearchFilterAttribute systemFilter = new SearchFilterAttribute(systemIds, false,
    // SearchFilterEnumeration.SYSTEM.getKey());
    // searchFilterMap.put(SearchFilterEnumeration.SYSTEM.getKey(), systemFilter);
    // }
    // else {
    // final StorageSystem storageSystem = this.storageSystemRepository
    // .findByStorageSystemName(storageSystemName);
    // if (storageSystem == null) {
    // final ValidationError verror = new ValidationError();
    // verror.setErrorCode(HttpStatus.BAD_GATEWAY);
    // verror.setErrorDescription("Invalid Storage System Name");
    // throw verror;
    // }
    // systemIds = new ArrayList<Long>();
    // systemIds.add(storageSystem.getStorageSystemId());
    // final SearchFilterAttribute systemFilter = new SearchFilterAttribute(systemIds, true,
    // SearchFilterEnumeration.SYSTEM.getKey());
    // searchFilterMap.put(SearchFilterEnumeration.SYSTEM.getKey(), systemFilter);
    // }
    //
    // if (zoneName.equals("All")) {
    // zoneIds = this.storageSystemRepository.findByStorageSystemIdIn(systemIds).stream()
    // .map(system -> system.getZoneId()).distinct().collect(Collectors.toList());
    // final SearchFilterAttribute zoneFilter = new SearchFilterAttribute(zoneIds, false,
    // SearchFilterEnumeration.ZONE.getKey());
    // searchFilterMap.put(SearchFilterEnumeration.ZONE.getKey(), zoneFilter);
    // }
    // else {
    // final Zone zone = this.zoneRepository.findByZoneName(zoneName);
    // if (zone == null) {
    // final ValidationError verror = new ValidationError();
    // verror.setErrorCode(HttpStatus.BAD_GATEWAY);
    // verror.setErrorDescription("Invalid Zone Name");
    // throw verror;
    // }
    // zoneIds = new ArrayList<Long>();
    // zoneIds.add(zone.getZoneId());
    // final SearchFilterAttribute zoneFilter = new SearchFilterAttribute(zoneIds, true,
    // SearchFilterEnumeration.ZONE.getKey());
    // searchFilterMap.put(SearchFilterEnumeration.ZONE.getKey(), zoneFilter);
    // }
    //
    // if (!searchFilterMap.get(SearchFilterEnumeration.TYPE.getKey()).isFilter) {
    // final SearchQuery typeSearchQuery = this.dataSetUtil.buildTypeQuery(searchString);
    // final List<ElasticSearchStorageType> storageTypes = this.esTemplate.queryForList(typeSearchQuery,
    // ElasticSearchStorageType.class);
    // final List<Long> typeIdsFromStringSearch = storageTypes != null && storageTypes.size() > 0
    // ? storageTypes.stream().map(type -> type.getStorageTypeId()).collect(Collectors.toList())
    // : new ArrayList<Long>();
    // final List<Long> intersectionList = typeIds.stream().filter(typeIdsFromStringSearch::contains)
    // .collect(Collectors.toList());
    // typeIds = intersectionList.size() > 0 ? intersectionList : typeIds;
    // searchFilterMap.put(SearchFilterEnumeration.TYPE.getKey(),
    // new SearchFilterAttribute(typeIds,
    // searchFilterMap.get(SearchFilterEnumeration.TYPE.getKey()).isFilter,
    // SearchFilterEnumeration.TYPE.getKey()));
    // }
    //
    // if (!searchFilterMap.get(SearchFilterEnumeration.SYSTEM.getKey()).isFilter) {
    // if (!searchString.equals("")) {
    // final SearchQuery storeSearchQuery = this.dataSetUtil.buildSystemQuery(searchString);
    // final List<ElasticSearchStorageSystem> storageSystems = this.esTemplate.queryForList(storeSearchQuery,
    // ElasticSearchStorageSystem.class);
    // final List<Long> systemIdsFromStringSearch = storageSystems != null && storageSystems.size() > 0
    // ? storageSystems.stream().map(system -> system.getStorageSystemId())
    // .collect(Collectors.toList())
    // : new ArrayList<Long>();
    // final List<Long> intersectionList = systemIds.stream().filter(systemIdsFromStringSearch::contains)
    // .collect(Collectors.toList());
    // systemIds = intersectionList.size() > 0 ? intersectionList : systemIds;
    // searchFilterMap.put(SearchFilterEnumeration.SYSTEM.getKey(),
    // new SearchFilterAttribute(systemIds,
    // searchFilterMap.get(SearchFilterEnumeration.SYSTEM.getKey()).isFilter,
    // SearchFilterEnumeration.SYSTEM.getKey()));
    // }
    // }
    //
    // if (!searchFilterMap.get(SearchFilterEnumeration.ZONE.getKey()).isFilter) {
    // if (!searchString.equals("")) {
    // final SearchQuery zoneSearchQuery = this.dataSetUtil.buildZoneQuery(searchString);
    // final List<ElasticSearchZone> zones = this.esTemplate.queryForList(zoneSearchQuery,
    // ElasticSearchZone.class);
    // final List<Long> zoneIdsFromStringSearch = zones != null && zones.size() > 0
    // ? zones.stream().map(zone -> zone.getZoneId()).collect(Collectors.toList())
    // : new ArrayList<Long>();
    // final List<Long> intersectionList = zoneIds.stream().filter(zoneIdsFromStringSearch::contains)
    // .collect(Collectors.toList());
    // zoneIds = intersectionList.size() > 0 ? intersectionList : zoneIds;
    // searchFilterMap.put(SearchFilterEnumeration.ZONE.getKey(),
    // new SearchFilterAttribute(zoneIds,
    // searchFilterMap.get(SearchFilterEnumeration.ZONE.getKey()).isFilter,
    // SearchFilterEnumeration.ZONE.getKey()));
    // }
    // }
    //
    // // get related fields from dataset index by applying the system filter
    // final SearchQuery datasetSearchQuery = this.dataSetUtil.buildDatasetQuery(searchString, pageable,
    // searchFilterMap);
    //
    // if (systemIds != null && systemIds.size() > 0) {
    // final Page<ElasticSearchDataset> datasets = this.esDatasetRepository.search(datasetSearchQuery);
    // if (datasets.getContent() != null && datasets.getContent().size() > 0) {
    // for (final ElasticSearchDataset dataset : datasets.getContent()) {
    // this.dataSetUtil.populateZoneNameAndSystemName(systemMappings, typeMappings, dataset);
    // }
    // return datasets;
    // }
    // else {
    // final Page<ElasticSearchDataset> datasetsAtSystemLevel = this.dataSetUtil.buildDatasetQuery(
    // searchString, pageable, systemIds);
    // for (final ElasticSearchDataset dataset : datasetsAtSystemLevel.getContent()) {
    // this.dataSetUtil.populateZoneNameAndSystemName(systemMappings, typeMappings, dataset);
    // }
    // return datasetsAtSystemLevel;
    // }
    // }
    // else {
    // final Page<ElasticSearchDataset> emptyPage = new PageImpl<ElasticSearchDataset>(
    // new ArrayList<ElasticSearchDataset>(), pageable, 0);
    // return emptyPage;
    // }
    //
    // }

    @Override
    public DatasetWithAttributes getPendingDataset(final long dataSetId) throws ValidationError {

        final Gson gson = new Gson();
        final List<DatasetWithAttributes> datasets = new ArrayList<DatasetWithAttributes>();
        final Map<Long, StorageTypeAttributeKey> typeAttributeMap = this.storageTypeUtil.getStorageTypeAttributes();
        final Map<Long, StorageSystem> systemMappings = this.systemUtil.getStorageSystems();
        final List<Object[]> datasetObj = this.dataSetRepository.getDatasetDetails(dataSetId);
        if (datasetObj == null) {
            final ValidationError v = new ValidationError();
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("Invalid dataset ID");
            throw v;
        }

        for (final Object[] obj : datasetObj) {
            final long storageSystemId = ((Number) obj[10]).longValue();
            final long objectSchemaMapId = ((Number) obj[2]).longValue();
            final long datasetId = ((Number) obj[0]).longValue();
            final DatasetWithAttributes dwa = new DatasetWithAttributes(
                    datasetId, String.valueOf(obj[1]),
                    objectSchemaMapId, String.valueOf(obj[3]),
                    String.valueOf(obj[4]), String.valueOf(obj[5]),
                    String.valueOf(obj[6]), String.valueOf(obj[7]),
                    String.valueOf(obj[8]), storageSystemId,
                    String.valueOf(obj[11]), String.valueOf(obj[9]), String.valueOf(obj[13]), "", "");

            final String storageSystemName = systemMappings.get(storageSystemId).getStorageSystemName();
            final ObjectSchemaMap rawObject = this.schemaMapUtil.validateObjectSchemaMapId(objectSchemaMapId);
            final List<Schema> objectSchema = gson.fromJson(rawObject.getObjectSchemaInString(),
                    new TypeToken<List<Schema>>() {
                    }.getType());
            dwa.setObjectSchema(objectSchema);
            dwa.setCreatedTimestampOnStore(rawObject.getCreatedTimestampOnStore());
            dwa.setCreatedUserOnStore(rawObject.getCreatedUserOnStore());

            final List<StorageSystemAttributeValue> systemAttributes = this.systemAttributeValueRepository
                    .findByStorageSystemIdAndIsActiveYN(storageSystemId, ActiveEnumeration.YES.getFlag());
            dwa.setSystemAttributes(systemAttributes);

            final List<ObjectAttributeValue> objectAttributes = this.objectSchemaAttributeRepository
                    .findByObjectIdAndIsActiveYN(objectSchemaMapId, ActiveEnumeration.YES.getFlag());
            objectAttributes.forEach(attribute -> {
                final StorageTypeAttributeKey stak = typeAttributeMap.get(attribute.getStorageDsAttributeKeyId());
                attribute.setStorageDsAttributeKeyName(stak.getStorageDsAttributeKeyName());
            });
            dwa.setObjectAttributes(objectAttributes);

            final List<Long> pendingAttributeIds = this.dataSetUtil.populateMissingAttributes(objectSchemaMapId,
                    storageSystemId, systemMappings.get(storageSystemId).getStorageTypeId());
            final List<StorageTypeAttributeKey> pendingTypeAttributes = pendingAttributeIds.stream()
                    .map(pendingAttr -> typeAttributeMap.get(pendingAttr))
                    .collect(Collectors.toList());
            dwa.setPendingTypeAttributes(pendingTypeAttributes);

            final List<ObjectAttributeKeyValue> customAttributes = this.sackvr.findByObjectId(objectSchemaMapId);
            dwa.setCustomAttributes(customAttributes);

            if (storageSystemName.contains("Teradata")) {
                this.schemaDatasetUtil.setTeradataSchemaAndTagsForDataset(dwa, storageSystemId,
                        rawObject.getContainerName(), rawObject.getObjectName());

            }
            else {
                this.bodhiDatasetUtil.setDatasetSchemaAndTagsForDataset(dwa, storageSystemId,
                        rawObject.getContainerName(), rawObject.getObjectName());
            }

            final List<DatasetOwnershipMap> owners = this.ownershipRepository
                    .findByStorageSystemIdAndContainerNameAndObjectName(rawObject.getStorageSystemId(),
                            rawObject.getContainerName(), rawObject.getObjectName());
            dwa.setOwners(owners);

            datasets.add(dwa);
        }
        return datasets != null && datasets.size() == 1 ? datasets.get(0) : null;
    }

    @Override
    public Page<Dataset> getAllDatasetsByTypeAndSystem(final String datasetSubstring, final String storageTypeName,
            final String storageSystemName, final String zoneName, final Pageable pageable) throws ValidationError {

        if (!zoneName.equals("All") && !storageTypeName.equals("All")) {
            final StorageType storageType = this.storageTypeRepository.findByStorageTypeName(storageTypeName);
            final List<StorageSystem> storageSystems = this.storageSystemRepository
                    .findByStorageTypeId(storageType.getStorageTypeId());
            final Map<Long, Zone> zones = this.zoneUtil.getZones();
            this.typeZoneList.clear();
            for (int i = 0; i < storageSystems.size(); i++) {
                final String name = zones.get(storageSystems.get(i).getZoneId()).getZoneName();
                if (name.equals(zoneName)) {
                    this.typeZoneList.add(storageSystems.get(i));
                }
            }
            this.typeZoneSystemList.clear();
            for (int i = 0; i < this.typeZoneList.size(); i++) {
                if (this.typeZoneList.get(i).getStorageSystemName().equals(storageSystemName)) {
                    this.typeZoneSystemList.add(this.typeZoneList.get(i));
                }
            }
            if (storageSystemName.equals("All")) {
                return this.dataSetUtil.getAllDatasetForSystemIds(datasetSubstring, this.typeZoneList, pageable,
                        storageType);
            }
            else {
                return this.dataSetUtil.getAllDatasetForSystemIds(datasetSubstring, this.typeZoneSystemList, pageable,
                        storageType);
            }

        }
        if (storageTypeName.equals("All") && !zoneName.equals("All")) {
            final Zone zone = this.zoneRepository.findByZoneName(zoneName);
            final List<StorageSystem> storageSystems = this.storageSystemRepository
                    .findByZoneId(zone.getZoneId());
            this.zoneStorageSystemList.clear();

            for (int i = 0; i < storageSystems.size(); i++) {
                if (storageSystems.get(i).getStorageSystemName().equals(storageSystemName)) {
                    this.zoneStorageSystemList.add(storageSystems.get(i));
                }
            }
            if (storageSystemName.equals("All")) {
                return this.dataSetUtil.getAllDatasetForZoneSystemIds(datasetSubstring, storageSystems, pageable);
            }
            else {
                return this.dataSetUtil.getAllDatasetForZoneSystemIds(datasetSubstring, this.zoneStorageSystemList,
                        pageable);
            }
        }

        if (!storageTypeName.equals("All") && zoneName.equals("All")) {
            final StorageType storageType = this.storageTypeRepository.findByStorageTypeName(storageTypeName);
            final List<StorageSystem> storageSystems = this.storageSystemRepository
                    .findByStorageTypeId(storageType.getStorageTypeId());
            if (storageSystemName.equals("All")) {
                return this.dataSetUtil.getAllDatasetForSystemIds(datasetSubstring, storageSystems, pageable,
                        storageType);
            }
            else {
                return this.dataSetUtil.getAllDatasetForSystem(datasetSubstring, storageSystemName, pageable);
            }
        }
        else {
            if (storageSystemName.equals("All")) {
                return this.dataSetUtil.getDatasetsForAllTypesAndSystems(datasetSubstring, pageable);
            }
            else {
                return this.dataSetUtil.getAllDatasetForSystem(datasetSubstring, storageSystemName, pageable);
            }
        }
    }

    @Override
    public Page<Dataset> getAllDeletedDatasetsByTypeAndSystem(final String datasetSubstring,
            final String storageTypeName, final String storageSystemName, final Pageable pageable)
            throws ValidationError {

        if (!storageTypeName.equals("All")) {
            final StorageType storageType = this.storageTypeRepository.findByStorageTypeName(storageTypeName);
            final List<StorageSystem> storageSystems = this.storageSystemRepository
                    .findByStorageTypeId(storageType.getStorageTypeId());
            final List<Long> storageSystemIds = storageSystems.stream()
                    .map(storageSystem -> storageSystem.getStorageSystemId()).collect(Collectors.toList());
            if (storageSystemName.equals("All")) {
                return this.dataSetUtil.getAllDeletedDatasetsForSystemIds(datasetSubstring, storageSystemIds, pageable,
                        storageType);
            }
            else {
                return this.dataSetUtil.getAllDeletedDatasetsForSystem(datasetSubstring, storageSystemName, pageable);
            }
        }
        else {
            if (storageSystemName.equals("All")) {
                return this.dataSetUtil.getDeletedDatasetsForAllTypesAndSystems(datasetSubstring, pageable);
            }
            else {
                return this.dataSetUtil.getAllDeletedDatasetsForSystem(datasetSubstring, storageSystemName, pageable);
            }
        }
    }

    @Override
    public Page<Dataset> getPendingDatasets(final String datasetSubstring, final String storageTypeName,
            final String storageSystemName, final Pageable pageable) throws ValidationError {

        if (!storageTypeName.equals("All")) {
            final StorageType storageType = this.storageTypeRepository.findByStorageTypeName(storageTypeName);
            final List<StorageSystem> storageSystems = this.storageSystemRepository
                    .findByStorageTypeId(storageType.getStorageTypeId());
            final List<Long> storageSystemIds = storageSystems.stream()
                    .map(storageSystem -> storageSystem.getStorageSystemId()).collect(Collectors.toList());
            if (storageSystemName.equals("All")) {
                return this.dataSetUtil.getAllPendingDatasetsForSystemIds(datasetSubstring, storageSystemIds, pageable,
                        storageType);
            }
            else {
                return this.dataSetUtil.getAllPendingDatasetsForSystem(datasetSubstring, storageSystemName, pageable);
            }
        }
        else {
            if (storageSystemName.equals("All")) {
                return this.dataSetUtil.getPendingDatasetsForAllTypesAndSystems(datasetSubstring, pageable);
            }
            else {
                return this.dataSetUtil.getAllPendingDatasetsForSystem(datasetSubstring, storageSystemName, pageable);
            }
        }

    }

    @Override
    public Dataset getDataSetById(final long dataSetId) throws ValidationError {
        Dataset dataSet = this.dataSetRepository.findById(dataSetId).orElse(null);
        if (dataSet == null) {
            final ValidationError v = new ValidationError();
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("Dataset Id is empty");
            throw v;
        }
        final Map<Long, StorageSystem> systemMappings = this.systemUtil.getStorageSystems();
        final Map<Long, Cluster> clusterMap = new HashMap<Long, Cluster>();
        this.clusterRepository.findAll().forEach(cluster -> {
            clusterMap.put(cluster.getClusterId(), cluster);
        });
        if (dataSet != null) {
            dataSet = this.dataSetUtil.getDataSet(dataSet);
            dataSet.setStorageSystemName(systemMappings.get(dataSet.getStorageSystemId()).getStorageSystemName());
        }
        return dataSet;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED, readOnly = false, rollbackFor = { ValidationError.class,
            ConstraintViolationException.class, DataIntegrityViolationException.class, IOException.class,
            InterruptedException.class, ExecutionException.class })
    public Dataset addDataset(final Dataset dataset)
            throws ValidationError, IOException, InterruptedException, ExecutionException {

        Dataset tempDataset = new Dataset();
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final String time = sdf.format(timestamp);
        final String createdUser = dataset.getCreatedUser();
        final String isAutoRegistered = dataset.getIsAutoRegistered();
        final String containerName = dataset.getStorageContainerName();
        final String objectName = dataset.getObjectName();
        boolean areAttributesPresent = true;
        final long storageSystemId = dataset.getStorageSystemId();
        this.systemUtil.validateStorageSystem(storageSystemId);
        final StorageType storageType = this.systemUtil.getStorageType(storageSystemId);
        final String storageTypeName = storageType.getStorageTypeName();
        final ValidationError v = new ValidationError();
        final Map<Long, Cluster> clusterMap = new HashMap<Long, Cluster>();
        this.clusterRepository.findAll().forEach(cluster -> {
            clusterMap.put(cluster.getClusterId(), cluster);
        });

        try {
            // this.userUtil.validateUser(dataset.getUserId());
            this.clusterUtil.validateClusters(dataset.getClusters());
            final ObjectSchemaMap objectSchemaMap = this.schemaMapUtil
                    .validateSystemContainerObjectMapping(storageSystemId, containerName, objectName);
            final long objectSchemaMapId = objectSchemaMap.getObjectId();
            dataset.setObjectSchemaMapId(objectSchemaMapId);

            if (storageTypeName.equalsIgnoreCase("Hbase")) {
                final StorageSystem storageSystem = this.storageSystemUtil.validateStorageSystem(storageSystemId);
                final String storageSystemName = storageSystem.getStorageSystemName();
                clusterMap.forEach((clusterId, cluster) -> {
                    if (storageSystemName.toLowerCase().contains(cluster.getClusterName().toLowerCase())) {
                        final List<Long> tempClusters = new ArrayList<Long>();
                        tempClusters.add(clusterId);
                        dataset.setClusters(tempClusters);
                    }
                });
            }

            areAttributesPresent = this.dataSetUtil.validateAttributes(objectSchemaMapId, storageSystemId,
                    storageType.getStorageTypeId());

            tempDataset = new Dataset(dataset.getStorageDataSetName(), dataset.getStorageDataSetName(), containerName,
                    databaseName, dataset.getStorageDataSetDescription(), ActiveEnumeration.YES.getFlag(), createdUser,
                    time, createdUser, time, dataset.getUserId(), isAutoRegistered, dataset.getObjectSchemaMapId(),
                    dataset.getStorageSystemId());

            this.dataSetRepository.save(tempDataset);
        }
        catch (final DataIntegrityViolationException e) {
            v.setErrorCode(HttpStatus.CONFLICT);
            v.setErrorDescription("Dataset name is duplicated");
            throw v;
        }
        catch (final ConstraintViolationException e) {
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("Dataset name is empty");
            throw v;
        }

        // insert into pc_storage_dataset_system table
        final DatasetStorageSystem dsw = new DatasetStorageSystem();
        dsw.setIsActiveYN(ActiveEnumeration.YES.getFlag());
        dsw.setCreatedUser(createdUser);
        dsw.setCreatedTimestamp(time);
        dsw.setUpdatedUser(createdUser);
        dsw.setUpdatedTimestamp(time);
        dsw.setStorageDataSetId(tempDataset.getStorageDataSetId());
        dsw.setStorageSystemId(storageSystemId);
        this.datasetStorageSystemRepository.save(dsw);

        // insert into changeLog table on creation of new datasets
        final String curr = "{\"value\": \"" + dataset.getStorageDataSetName() + "\", \"username\": \"" + createdUser
                + "\"}";
        final DatasetChangeLog dcl = new DatasetChangeLog(tempDataset.getStorageDataSetId(), "C", "DATASET", "{}", curr,
                time);
        this.changeLogRepository.save(dcl);

        // update the object_schema_cluster_map table if its via Auto registration
        if (isAutoRegistered.equals(ActiveEnumeration.YES.getFlag())) {
            this.schemaMapUtil.updateObjectAutoRegistrationStatus(dataset.getObjectSchemaMapId(), areAttributesPresent,
                    storageTypeName);
        }

        if (this.isEsWriteEnabled.equals("true")) {
            this.dataSetUtil.upsertDataset(this.esDatasetIndex, this.esType, tempDataset, this.esTemplate);
        }

        return tempDataset;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED, readOnly = false, rollbackFor = {
            TransactionSystemException.class, DataIntegrityViolationException.class, IndexOutOfBoundsException.class,
            ValidationError.class })
    public Dataset updateDataSet(final Dataset dataSet)
            throws ValidationError, IOException, InterruptedException, ExecutionException {

        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final ValidationError v = new ValidationError();
        final long dataSetId = dataSet.getStorageDataSetId();
        final String time = sdf.format(timestamp);
        Dataset tempDataSet = this.dataSetRepository.findById(dataSetId).orElse(null);
        final List<DatasetChangeLog> changeLogList = this.changeLogRepository
                .findByStorageDataSetIdAndChangeColumnType(dataSetId, "DATASET");
        final DatasetChangeLog tempChangeLogDataset = changeLogList.get(changeLogList.size() - 1);
        if (tempDataSet == null) {
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("Dataset ID is invalid");
            throw v;
        }
        try {
            tempDataSet.setUpdatedUser(dataSet.getUpdatedUser());
            tempDataSet.setUpdatedTimestamp(time);
            this.s1.setNextChain(this.s2);
            this.s2.setNextChain(this.s4);
            this.s1.validate(dataSet, tempDataSet);
            logger.info(dataSet.getStorageDataSetDescription());
            tempDataSet = this.dataSetRepository.save(tempDataSet);

            // insert into changeLog table when dataset names are modified
            final String curr = "{\"value\": \"" + tempDataSet.getStorageDataSetName() + "\", \"username\": \""
                    + dataSet.getUpdatedUser()
                    + "\"}";
            final DatasetChangeLog dcl = new DatasetChangeLog(tempChangeLogDataset.getStorageDataSetId(), "M",
                    "DATASET", tempChangeLogDataset.getColumnCurrValInString(), curr,
                    time);
            this.changeLogRepository.save(dcl);
        }
        catch (final TransactionSystemException e) {
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("Dataset name is empty");
            throw v;
        }
        catch (final DataIntegrityViolationException e) {
            v.setErrorCode(HttpStatus.CONFLICT);
            v.setErrorDescription("Dataset name is duplicated");
            throw v;
        }

        final DatasetStorageSystem dss = this.datasetStorageSystemRepository.findByStorageDataSetId(dataSetId);
        tempDataSet.setStorageSystemId(dss.getStorageSystemId());

        if (this.isEsWriteEnabled.equals("true")) {
            this.dataSetUtil.upsertDataset(this.esDatasetIndex, this.esType, tempDataSet, this.esTemplate);
        }
        return tempDataSet;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED, readOnly = false, rollbackFor = {
            TransactionSystemException.class, DataIntegrityViolationException.class,
            ValidationError.class, IOException.class, InterruptedException.class, ExecutionException.class })
    public Dataset deleteDataSet(final long dataSetId)
            throws ValidationError, IOException, InterruptedException, ExecutionException {

        final ValidationError v = new ValidationError();
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());

        // update pc_storage_dataset table
        final Dataset dataSet = this.dataSetRepository.findById(dataSetId).orElse(null);
        if (dataSet == null) {
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("DataSet ID is invalid");
            throw v;
        }
        dataSet.setUpdatedTimestamp(sdf.format(timestamp));
        dataSet.setIsActiveYN(ActiveEnumeration.NO.getFlag());
        this.dataSetRepository.save(dataSet);

        // update pc_storage_dataset_system table
        final DatasetStorageSystem dsw = this.datasetStorageSystemRepository.findByStorageDataSetId(dataSetId);
        dsw.setUpdatedTimestamp(sdf.format(timestamp));
        dsw.setIsActiveYN(ActiveEnumeration.NO.getFlag());
        this.datasetStorageSystemRepository.save(dsw);

        // update pc_storage_dataset_change_log table
        final List<DatasetChangeLog> changeLogList = this.changeLogRepository
                .findByStorageDataSetIdAndChangeColumnType(dataSetId, "DATASET");
        final DatasetChangeLog tempChangeLogDataset = changeLogList.get(changeLogList.size() - 1);

        final DatasetChangeLog dcl = new DatasetChangeLog(dataSetId, "D",
                "DATASET", tempChangeLogDataset.getColumnCurrValInString(), "{}",
                sdf.format(timestamp));
        this.changeLogRepository.save(dcl);

        if (this.isEsWriteEnabled.equals("true")) {
            this.dataSetUtil.upsertDataset(this.esDatasetIndex, this.esType, dataSet, this.esTemplate);
        }

        return dataSet;
    }

    @Override
    public DatasetWithAttributes getDataSetByName(final String dataSetName) throws ValidationError {

        final Gson gson = new Gson();
        final Map<Long, StorageSystem> storageSystemMap = this.systemUtil.getStorageSystems();
        final Map<Long, Cluster> clusterMap = new HashMap<Long, Cluster>();
        this.clusterRepository.findAll().forEach(cluster -> {
            clusterMap.put(cluster.getClusterId(), cluster);
        });
        final DatasetWithAttributes dataset = new DatasetWithAttributes();
        final List<Dataset> registeredDatasets = this.dataSetRepository
                .findByStorageDataSetNameAndIsActiveYN(dataSetName,
                        ActiveEnumeration.YES.getFlag());
        if (registeredDatasets == null || registeredDatasets.size() == 0) {
            return new DatasetWithAttributes();
        }
        final Dataset registeredDataset = registeredDatasets.size() == 1
                ? registeredDatasets.get(0)
                : registeredDatasets.stream()
                        .filter(tempDataset -> tempDataset.getIsAutoRegistered()
                                .equals(ActiveEnumeration.YES.getFlag()))
                        .collect(Collectors.toList()).get(0);
        final long storageSystemId = registeredDataset.getStorageSystemId();
        final long objectSchemaMapId = registeredDataset.getObjectSchemaMapId();
        final ObjectSchemaMap rawObject = this.schemaMapUtil
                .validateObjectSchemaMapId(objectSchemaMapId);
        final StorageSystem storageSystem = storageSystemMap.get(registeredDataset.getStorageSystemId());
        final String storageSystemName = storageSystem.getStorageSystemName();
        dataset.setStorageSystemId(storageSystemId);
        dataset.setStorageSystemName(storageSystemName);

        dataset.setStorageDataSetId(registeredDataset.getStorageDataSetId());
        dataset.setStorageDataSetName(registeredDataset.getStorageDataSetName());
        dataset.setObjectSchemaMapId(objectSchemaMapId);
        dataset.setIsAutoRegistered(registeredDataset.getIsAutoRegistered());
        dataset.setCreatedUser(registeredDataset.getCreatedUser());
        dataset.setUpdatedUser(registeredDataset.getUpdatedUser());
        dataset.setCreatedTimestamp(registeredDataset.getUpdatedTimestamp());
        dataset.setUpdatedTimestamp(registeredDataset.getUpdatedTimestamp());
        dataset.setIsActiveYN(registeredDataset.getIsActiveYN());
        dataset.setCreatedTimestampOnStore(rawObject.getCreatedTimestampOnStore());
        dataset.setCreatedUserOnStore(rawObject.getCreatedUserOnStore());
        dataset.setQuery(rawObject.getQuery());
        final String schema = rawObject.getObjectSchemaInString();
        final List<Schema> modifiedSchema = gson.fromJson(schema, new TypeToken<List<Schema>>() {
        }.getType());

        // ADD THE LOGIC TO SHOWCASE THE MIN MAX FOR TERADATA ONLY
        if (modifiedSchema != null) {
            final Schema minimumColumn = modifiedSchema.stream()
                    .min(Comparator.comparing(Schema::getColumnIndex))
                    .orElse(new Schema());
            final int minimumColumnIndex = minimumColumn.getColumnIndex();
            modifiedSchema.forEach(column -> column.setColumnIndex(column.getColumnIndex() - minimumColumnIndex));
            modifiedSchema.sort(Comparator.comparing(Schema::getColumnIndex));
        }

        dataset.setObjectSchema(modifiedSchema);

        final List<StorageSystemAttributeValue> systemAttributes = this.systemUtil
                .getAttributes(storageSystemId);
        dataset.setSystemAttributes(systemAttributes);

        final Map<Long, StorageTypeAttributeKey> storageTypeAttributeMap = this.storageTypeUtil
                .getStorageTypeAttributes();
        final List<ObjectAttributeValue> objectAttributes = this.objectSchemaAttributeRepository
                .findByObjectIdAndIsActiveYN(objectSchemaMapId, ActiveEnumeration.YES.getFlag());
        objectAttributes.forEach(attribute -> {
            final StorageTypeAttributeKey typeAttributeKey = storageTypeAttributeMap
                    .get(attribute.getStorageDsAttributeKeyId());
            attribute.setStorageDsAttributeKeyName(typeAttributeKey.getStorageDsAttributeKeyName());
        });
        dataset.setObjectAttributes(objectAttributes);

        if (this.dataSetUtil.validateAttributes(objectSchemaMapId, storageSystemId, storageSystem.getStorageTypeId())) {
            dataset.setPendingTypeAttributes(new ArrayList<>());
        }
        else {
            final List<Long> pendingAttributeIds = this.dataSetUtil.populateMissingAttributes(objectSchemaMapId,
                    storageSystemId, storageSystem.getStorageTypeId());
            final Map<Long, StorageTypeAttributeKey> typeAttributeMap = this.storageTypeUtil.getStorageTypeAttributes();
            final List<StorageTypeAttributeKey> pendingTypeAttributes = pendingAttributeIds.stream()
                    .map(pendingAttr -> typeAttributeMap.get(pendingAttr))
                    .collect(Collectors.toList());
            dataset.setPendingTypeAttributes(pendingTypeAttributes);
        }

        final List<ObjectAttributeKeyValue> customAttributes = this.sackvr.findByObjectId(objectSchemaMapId);
        dataset.setCustomAttributes(customAttributes);

        if (storageSystem.getStorageSystemName().contains("Teradata")) {
            this.schemaDatasetUtil.setTeradataSchemaAndTagsForDataset(dataset, storageSystemId,
                    rawObject.getContainerName(), rawObject.getObjectName());

        }
        else {
            this.bodhiDatasetUtil.setDatasetSchemaAndTagsForDataset(dataset, storageSystemId,
                    rawObject.getContainerName(), rawObject.getObjectName());
        }

        final List<DatasetOwnershipMap> owners = this.ownershipRepository
                .findByStorageSystemIdAndContainerNameAndObjectName(storageSystemId, rawObject.getContainerName(),
                        rawObject.getObjectName());
        dataset.setOwners(owners);
        return dataset;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED, readOnly = false, rollbackFor = { ValidationError.class })
    public void updateDatasetWithPendingAttributes(final DatasetWithAttributes dataset) throws ValidationError {

        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final String time = sdf.format(timestamp);
        final ValidationError v = new ValidationError();
        final Dataset retreivedDataset = this.dataSetRepository.findById(dataset.getStorageDataSetId())
                .orElse(null);
        if (retreivedDataset == null) {
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("Invalid Dataset ID");
            throw v;
        }
        // insert into pc_storage_system_attribute_value table
        try {
            final List<StorageSystemAttributeValue> systemAttributes = dataset.getSystemAttributes();
            systemAttributes.forEach(systemAttribute -> {
                final StorageSystemAttributeValue retrievedAttributeValue = this.systemAttributeValueRepository
                        .findByStorageSystemIdAndStorageDataSetAttributeKeyIdAndStorageSystemAttributeValue(
                                systemAttribute.getStorageSystemID(), systemAttribute.getStorageDataSetAttributeKeyId(),
                                systemAttribute.getStorageSystemAttributeValue());
                if (retrievedAttributeValue == null) {
                    systemAttribute.setUpdatedTimestamp(time);
                    systemAttribute.setCreatedTimestamp(time);
                    systemAttribute.setUpdatedUser(dataset.getCreatedUser());
                    systemAttribute.setCreatedUser(dataset.getCreatedUser());
                    systemAttribute.setIsActiveYN(ActiveEnumeration.YES.getFlag());
                    this.systemAttributeValueRepository.save(systemAttribute);
                }
                else {
                    retrievedAttributeValue.setUpdatedUser(dataset.getCreatedUser());
                    retrievedAttributeValue.setUpdatedTimestamp(time);
                    retrievedAttributeValue
                            .setStorageSystemAttributeValue(systemAttribute.getStorageSystemAttributeValue());
                    this.systemAttributeValueRepository.save(retrievedAttributeValue);
                }
            });
        }
        catch (final DataIntegrityViolationException e) {
            v.setErrorCode(HttpStatus.CONFLICT);
            v.setErrorDescription("Conflicting storage system ID or Type Attribute Key");
            throw v;
        }
        catch (final ConstraintViolationException e) {
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("System Attribute value is empty");
            throw v;
        }

        // insert into pc_storage_object_attribute_value table
        try {
            final List<ObjectAttributeValue> objectAttributes = dataset.getObjectAttributes();
            objectAttributes.forEach(objectAttribute -> {
                final ObjectAttributeValue retrievedAttributeValue = this.objectSchemaAttributeRepository
                        .findByObjectIdAndStorageDsAttributeKeyIdAndObjectAttributeValue(
                                objectAttribute.getObjectId(), objectAttribute.getStorageDsAttributeKeyId(),
                                objectAttribute.getObjectAttributeValue());
                if (retrievedAttributeValue == null) {
                    objectAttribute.setUpdatedTimestamp(time);
                    objectAttribute.setCreatedTimestamp(time);
                    objectAttribute.setUpdatedUser(dataset.getCreatedUser());
                    objectAttribute.setCreatedUser(dataset.getCreatedUser());
                    objectAttribute.setIsActiveYN(ActiveEnumeration.YES.getFlag());
                    this.objectSchemaAttributeRepository.save(objectAttribute);
                }
                else {
                    retrievedAttributeValue.setUpdatedUser(dataset.getCreatedUser());
                    retrievedAttributeValue.setUpdatedTimestamp(time);
                    retrievedAttributeValue.setObjectAttributeValue(objectAttribute.getObjectAttributeValue());
                    this.objectSchemaAttributeRepository.save(retrievedAttributeValue);
                }

            });
        }
        catch (final DataIntegrityViolationException e) {
            v.setErrorCode(HttpStatus.CONFLICT);
            v.setErrorDescription("Conflicting object ID or Type Attribute Key");
            throw v;
        }
        catch (final ConstraintViolationException e) {
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("Object Attribute Value is empty");
            throw v;
        }

        // update pc_object_schema_map table
        if (retreivedDataset.getIsAutoRegistered().equals(ActiveEnumeration.YES.getFlag())) {
            final ObjectSchemaMap schemaMap = this.schemaMapUtil
                    .validateObjectSchemaMapId(retreivedDataset.getObjectSchemaMapId());
            schemaMap.setIsRegistered(ActiveEnumeration.YES.getFlag());
            schemaMap.setUpdatedTimestamp(time);
            this.objectSchemaMapRepository.save(schemaMap);
        }

        // ** TODO: Remove Commented Code **
        // insert into pc_storage_change_log_registered table
        // final List<Long> clusters = this.clusterUtil.getAllClusters().stream().map(cluster -> cluster.getClusterId())
        // .collect(Collectors.toList());
        // clusters.forEach(clusterId -> {
        // final DatasetChangeLogRegistered changeLog = new DatasetChangeLogRegistered();
        // changeLog.setStorageDataSetId(retreivedDataset.getStorageDataSetId());
        // changeLog.setStorageDataSetName(retreivedDataset.getStorageDataSetName());
        // changeLog.setStorageDataSetAliasName(retreivedDataset.getStorageDataSetAliasName() + "_");
        // changeLog.setStorageContainerName(retreivedDataset.getStorageContainerName());
        // changeLog.setStorageDatabaseName(retreivedDataset.getStorageDatabaseName());
        // changeLog.setStorageDataSetDescription(retreivedDataset.getStorageDataSetDescription());
        // changeLog.setStorageSystemId(dataset.getStorageSystemId());
        // changeLog.setStorageClusterId(clusterId);
        // changeLog.setUserId(retreivedDataset.getUserId());
        // changeLog.setStorageDatasetChangeType(ChangeEnumration.CREATE.getFlag());
        // changeLog.setCreatedTimestamp(time);
        // changeLog.setUpdatedTimestamp(time);
        // final ObjectSchemaMap schemaMap = this.objectSchemaMapRepository.findOne(dataset.getObjectSchemaMapId());
        // final String query = schemaMap.getQuery() == null ? "" : schemaMap.getQuery();
        // final String modifiedQuery = query.replace("TABLENAME", retreivedDataset.getStorageDataSetName())
        // .replace("DATABASE", databaseName);
        // changeLog.setStorageDatasetQuery(modifiedQuery);
        // changeLog.setCreatedUser(retreivedDataset.getCreatedUser());
        // changeLog.setUpdatedUser(retreivedDataset.getUpdatedUser());
        // this.changeLogRegisteredRepository.save(changeLog);
        // });
    }

    // ** TODO: Remove Commented Code **
    // @Override
    // public List<DatasetChangeLogRegistered> getDatasetChangeLogs(final long storageClusterId) {
    // final List<DatasetChangeLogRegistered> changeLogs = this.changeLogRegisteredRepository
    // .findChanges(storageClusterId);
    // return changeLogs;
    // }
    @Override
    public Map<String, List<String>> getTimelineDimensions() {
        final Map<String, List<String>> timelineFilterMap = new HashMap<String, List<String>>();
        final TimelineEnumeration[] list = TimelineEnumeration.values();
        for (final TimelineEnumeration timelineDimension : list) {
            final List<String> result = new ArrayList<String>();
            result.add(timelineDimension.getFlag());
            result.add(timelineDimension.getDescription());
            result.add(timelineDimension.getColor());
            timelineFilterMap.put(timelineDimension.getDescription(), result);
        }
        return timelineFilterMap;
    }

    @Override
    public List<DatasetChangeLog> getChangeLogsByDataSetId(final long datasetId) {
        final List<DatasetChangeLog> changeLogs = this.changeLogRepository
                .findByStorageDataSetId(datasetId);
        return changeLogs;
    }

    @Override
    public List<DatasetChangeLog> getChangeLogsByDataSetIdAndChangeColumnType(final long datasetId,
            final String changeType) {
        final List<DatasetChangeLog> changeLogs = this.changeLogRepository
                .findByStorageDataSetIdAndChangeColumnType(datasetId, changeType);
        return changeLogs;
    }

    @Override
    public long getDatasetCount() {
        final long datasetCount = this.dataSetRepository.count();
        return datasetCount;
    }

}
