package com.paypal.udc.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.paypal.udc.cache.ObjectSchemaMapCache;
import com.paypal.udc.dao.dataset.DatasetRepository;
import com.paypal.udc.dao.dataset.DatasetStorageSystemRepository;
import com.paypal.udc.dao.dataset.PageableDatasetRepository;
import com.paypal.udc.dao.objectschema.ObjectSchemaAttributeValueRepository;
import com.paypal.udc.dao.storagesystem.StorageSystemAttributeValueRepository;
import com.paypal.udc.dao.storagesystem.StorageSystemRepository;
import com.paypal.udc.dao.storagetype.StorageTypeAttributeKeyRepository;
import com.paypal.udc.dao.storagetype.StorageTypeRepository;
import com.paypal.udc.dao.teradatapolicy.TeradataPolicyRepository;
import com.paypal.udc.entity.Cluster;
import com.paypal.udc.entity.Zone;
import com.paypal.udc.entity.dataset.Dataset;
import com.paypal.udc.entity.dataset.DatasetChangeLogRegistered;
import com.paypal.udc.entity.dataset.DatasetStorageSystem;
import com.paypal.udc.entity.dataset.DatasetWithAttributes;
import com.paypal.udc.entity.dataset.sampledata.LivyPostResponse;
import com.paypal.udc.entity.dataset.sampledata.LivySessionAvailabilityResponse;
import com.paypal.udc.entity.dataset.sampledata.LivySessionResponse;
import com.paypal.udc.entity.objectschema.ObjectAttributeValue;
import com.paypal.udc.entity.objectschema.ObjectSchemaMap;
import com.paypal.udc.entity.objectschema.Schema;
import com.paypal.udc.entity.rangerpolicy.DerivedPolicy;
import com.paypal.udc.entity.storagesystem.StorageSystem;
import com.paypal.udc.entity.storagesystem.StorageSystemAttributeValue;
import com.paypal.udc.entity.storagetype.StorageType;
import com.paypal.udc.entity.storagetype.StorageTypeAttributeKey;
import com.paypal.udc.entity.teradatapolicy.TeradataPolicy;
import com.paypal.udc.exception.ValidationError;
import com.paypal.udc.util.enumeration.ActiveEnumeration;


@Component
public class DatasetUtil {

    final static Logger logger = LoggerFactory.getLogger(DatasetUtil.class);

    final String protocol = "http://";
    final String sessionEndpoint = "/sessions";
    final String statementEndpoint = "/sessions/" + "$sessionName$" + "/statements";
    static final String teradataType = "teradata";
    static final String hbaseType = "hbase";
    static final String hiveType = "hive";

    @Autowired
    private DatasetStorageSystemRepository datasetSystemRepository;

    @Autowired
    private StorageSystemRepository storageSystemRepository;

    @Autowired
    private StorageTypeRepository storageTypeRepository;

    @Autowired
    private StorageTypeAttributeKeyRepository storageTypeAttributeRepository;

    @Autowired
    private DatasetRepository dataSetRepository;

    @Autowired
    private PageableDatasetRepository pageDatasetRepository;

    @Autowired
    private StorageSystemUtil systemUtil;

    @Autowired
    private ZoneUtil zoneUtil;

    @Autowired
    private StorageSystemAttributeValueRepository systemAttributeValueRepository;

    @Autowired
    private ObjectSchemaMapCache objectSchemaMapCache;

    @Autowired
    private TeradataPolicyRepository teradataPolicyRepository;

    @Autowired
    private ObjectSchemaAttributeValueRepository objectSchemaAttributeRepository;

    @Autowired
    private RangerPolicyUtil rangerPolicyUtil;

    @Autowired
    private ClusterUtil clusterUtil;

    private List<String> restrictedColumns = new ArrayList<String>();

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

    public DatasetChangeLogRegistered populateDSChangeLog(final String createdUser, final String time,
            final Dataset dataSet, final String changeType, final long clusterId) {
        final DatasetChangeLogRegistered changeLog = new DatasetChangeLogRegistered();
        changeLog.setStorageDataSetName(dataSet.getStorageDataSetName());
        changeLog.setStorageDataSetId(dataSet.getStorageDataSetId());
        changeLog.setStorageContainerName(dataSet.getStorageContainerName());
        changeLog.setStorageDatabaseName(dataSet.getStorageDatabaseName());
        changeLog.setStorageDataSetAliasName(dataSet.getStorageDataSetAliasName());
        changeLog.setStorageDataSetDescription(dataSet.getStorageDataSetDescription());
        changeLog.setStorageSystemId(dataSet.getStorageSystemId());
        changeLog.setUserId(dataSet.getUserId());
        changeLog.setStorageClusterId(clusterId);
        changeLog.setStorageDatasetChangeType(changeType);
        changeLog.setCreatedUser(createdUser);
        changeLog.setUpdatedUser(createdUser);
        changeLog.setCreatedTimestamp(time);
        changeLog.setUpdatedTimestamp(time);
        return changeLog;
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
            final Map<Long, StorageTypeAttributeKey> typeAttributeMap) {

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

        final ObjectSchemaMap schemaMap = this.objectSchemaMapCache.getObject(ds.getObjectSchemaMapId());
        dataset.setQuery(schemaMap.getQuery());
        final String schema = schemaMap.getObjectSchemaInString();
        final List<Schema> modifiedSchema = gson.fromJson(schema, new TypeToken<List<Schema>>() {
        }.getType());
        dataset.setObjectSchema(modifiedSchema);
        dataset.setStorageSystemName(systemMappings.get(storageSystemId).getStorageSystemName());

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

        return dataset;
    }

    public Page<Dataset> getPendingDatasetsForAllTypesAndSystems(final String datasetStr, final Pageable pageable) {

        final String datasetSubString = datasetStr.equals("All") ? "" : datasetStr;
        final Map<Long, StorageSystem> systemMappings = this.systemUtil.getStorageSystems();
        final Page<Dataset> pagedDatasets = this.pageDatasetRepository
                .findByIsActiveYNAndStorageDataSetNameContaining(ActiveEnumeration.YES.getFlag(), datasetSubString,
                        pageable);
        pagedDatasets.getContent().stream().forEach(dataset -> {
            final long storageSystemId = this.objectSchemaMapCache.getObject(dataset.getObjectSchemaMapId())
                    .getStorageSystemId();
            final StorageSystem storageSystem = systemMappings.get(storageSystemId);
            final StorageType storageType = this.systemUtil.getStorageType(storageSystemId);
            this.populatePendingDataset(storageSystem, dataset, storageType);
            dataset.setStorageSystemId(storageSystemId);
        });
        return pagedDatasets;

    }

    public Page<Dataset> getAllDeletedDatasetsForSystem(final String datasetStr, final String storageSystemName,
            final Pageable pageable) {
        final StorageSystem storageSystem = this.storageSystemRepository
                .findByStorageSystemName(storageSystemName);
        final StorageType storageType = this.systemUtil.getStorageType(storageSystem.getStorageSystemId());
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

    public Page<Dataset> getDatasetsForAllTypesAndSystems(final String datasetStr, final Pageable pageable) {

        final Map<Long, Zone> zones = this.zoneUtil.getZones();
        final Map<Long, StorageSystem> systemMap = this.systemUtil.getStorageSystems();
        final String datasetSubString = datasetStr.equals("All") ? "" : datasetStr;
        final Page<Dataset> pagedDatasets = this.pageDatasetRepository
                .findByIsActiveYNAndStorageDataSetNameContaining(ActiveEnumeration.YES.getFlag(), datasetSubString,
                        pageable);
        pagedDatasets.getContent().stream().forEach(dataset -> {
            final ObjectSchemaMap objectSchemaMap = this.objectSchemaMapCache.getObject(dataset.getObjectSchemaMapId());
            final long storageSystemId = objectSchemaMap.getStorageSystemId();
            final StorageSystem storageSystem = systemMap.get(storageSystemId);
            final StorageType storageType = this.systemUtil.getStorageType(storageSystemId);
            this.populateAvailableDataset(storageSystem, dataset, storageType);
            dataset.setStorageSystemId(storageSystemId);
            this.setAccessControlForDataset(storageType.getStorageTypeName().toLowerCase(), storageSystem,
                    objectSchemaMap, dataset);
            dataset.setIsGimelCompatible(storageSystem.getIsGimelCompatible());
            dataset.setIsReadCompatible(storageSystem.getIsReadCompatible());
            dataset.setZoneName(zones.get(storageSystem.getZoneId()).getZoneName());
        });
        return pagedDatasets;
    }

    public Page<Dataset> getAllDatasetForSystem(final String datasetStr, final String storageSystemName,
            final Pageable pageable) {
        final String datasetSubstring = datasetStr.equals("All") ? "" : datasetStr;
        final Page<Dataset> pagedDatasets;
        final StorageSystem storageSystem = this.storageSystemRepository.findByStorageSystemName(storageSystemName);
        final StorageType storageType = this.storageTypeRepository.findOne(storageSystem.getStorageTypeId());
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
        pagedDatasets.getContent().stream().forEach(dataset -> {
            final ObjectSchemaMap objectSchemaMap = this.objectSchemaMapCache.getObject(dataset.getObjectSchemaMapId());
            this.setAccessControlForDataset(storageType.getStorageTypeName().toLowerCase(),
                    storageSystem, objectSchemaMap, dataset);
            this.populateAvailableDataset(storageSystem, dataset, storageType);
        });

        return pagedDatasets;
    }

    public Page<Dataset> getAllDatasetForSystemIds(final String datasetStr, final List<StorageSystem> storageSystems,
            final Pageable pageable, final StorageType storageType) {

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
        pagedDatasets.getContent().stream().forEach(dataset -> {
            final ObjectSchemaMap objectSchemaMap = this.objectSchemaMapCache
                    .getObject(dataset.getObjectSchemaMapId());

            final StorageSystem storageSystem = storageSystems.stream()
                    .filter(ss -> ss.getStorageSystemId() == objectSchemaMap.getStorageSystemId())
                    .collect(Collectors.toList()).get(0);
            this.setAccessControlForDataset(storageTypeName, storageSystem, objectSchemaMap,
                    dataset);
            this.populateAvailableDataset(storageSystem, dataset, storageType);
        });
        return pagedDatasets;
    }

    public void setAccessControlForDataset(final String storageTypeName, final StorageSystem storageSystem,
            final ObjectSchemaMap objectSchemaMap, final Dataset dataset) {
        switch (storageTypeName) {
            case teradataType:
                this.setAccessForTeradataDataset(dataset, storageSystem.getStorageSystemId(), objectSchemaMap);
                break;
            case hbaseType:
                this.setAccessForHbaseDataset(dataset, storageSystem.getRunningClusterId(), storageTypeName,
                        objectSchemaMap);
                break;
            case hiveType:
                this.setAccessForHiveDataset(dataset, storageSystem.getRunningClusterId(),
                        objectSchemaMap);
                break;
            default:
                dataset.setIsAccessControlled(ActiveEnumeration.NO.getFlag());
                break;
        }
    }

    private void setAccessForHbaseDataset(final Dataset dataset, final long clusterId, final String storageTypeName,
            final ObjectSchemaMap objectSchemaMap) {
        final List<ObjectAttributeValue> objectAttributes = this.objectSchemaAttributeRepository
                .findByObjectIdAndIsActiveYN(objectSchemaMap.getObjectId(),
                        ActiveEnumeration.YES.getFlag());
        final List<DerivedPolicy> allPolicies = new ArrayList<DerivedPolicy>();
        final List<TeradataPolicy> teradataPolicies = new ArrayList<TeradataPolicy>();
        if (objectAttributes == null || objectAttributes.size() == 0) {
            dataset.setIsAccessControlled(ActiveEnumeration.NO.getFlag());
        }
        else {
            for (final ObjectAttributeValue objectAttribute : objectAttributes) {
                final List<DerivedPolicy> derivedPolicies = this.rangerPolicyUtil
                        .computePoliciesByLocation(objectAttribute.getObjectAttributeValue(), storageTypeName,
                                clusterId);
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
            final ObjectSchemaMap objectSchemaMap) {
        final Map<Long, Cluster> clusters = this.clusterUtil.getClusters();
        final String clusterName = clusters.get(clusterId).getClusterName();
        final List<DerivedPolicy> allPolicies = new ArrayList<DerivedPolicy>();
        final List<TeradataPolicy> teradataPolicies = new ArrayList<TeradataPolicy>();
        final List<ObjectAttributeValue> objectAttributes = this.objectSchemaAttributeRepository
                .findByObjectIdAndIsActiveYN(objectSchemaMap.getObjectId(),
                        ActiveEnumeration.YES.getFlag());
        final String stringToReplace = "hdfs://" + clusterName;
        if (objectAttributes == null || objectAttributes.size() == 0) {
            dataset.setIsAccessControlled(ActiveEnumeration.NO.getFlag());

        }
        else {
            for (final ObjectAttributeValue objectAttribute : objectAttributes) {
                final String attributeValue = objectAttribute.getObjectAttributeValue().replace(stringToReplace, "");
                if (attributeValue.contains("/")) {
                    final List<DerivedPolicy> derivedPolicies = this.rangerPolicyUtil
                            .computePoliciesByLocation(attributeValue, "hdfs", clusterId);
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
            final ObjectSchemaMap objectSchemaMap) {
        final List<ObjectAttributeValue> objectAttributes = this.objectSchemaAttributeRepository
                .findByObjectIdAndIsActiveYN(objectSchemaMap.getObjectId(),
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

    public Page<Dataset> getDeletedDatasetsForAllTypesAndSystems(final String datasetStr, final Pageable pageable) {

        final Map<Long, StorageSystem> systemMappings = this.systemUtil.getStorageSystems();
        final String datasetSubString = datasetStr.equals("All") ? "" : datasetStr;
        final Page<Dataset> pagedDatasets = this.pageDatasetRepository
                .findByIsActiveYNAndStorageDataSetNameContaining(ActiveEnumeration.NO.getFlag(), datasetSubString,
                        pageable);
        pagedDatasets.getContent().stream().forEach(dataset -> {
            final long storageSystemId = this.objectSchemaMapCache.getObject(dataset.getObjectSchemaMapId())
                    .getStorageSystemId();
            final StorageSystem storageSystem = systemMappings.get(storageSystemId);
            final StorageType storageType = this.systemUtil.getStorageType(storageSystemId);
            this.populateAvailableDataset(storageSystem, dataset, storageType);
            dataset.setStorageSystemId(storageSystemId);
        });
        return pagedDatasets;

    }

    public Page<Dataset> getAllPendingDatasetsForSystem(final String datasetStr, final String storageSystemName,
            final Pageable pageable) {
        final String datasetSubString = datasetStr.equals("All") ? "" : datasetStr;
        final StorageSystem system = this.storageSystemRepository.findByStorageSystemName(storageSystemName);
        if (system == null) {
            return new PageImpl<Dataset>(new ArrayList<Dataset>(), pageable, 0);
        }
        final StorageType storageType = this.systemUtil.getStorageType(system.getStorageSystemId());
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
        dataset.setIsGimelCompatible(storageSystem.getIsGimelCompatible());
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

    public String getSessionAvailability(final String livyEndPoint, final long livyPort) throws ValidationError {
        final String getUrl = this.protocol + livyEndPoint + ":" + livyPort + this.sessionEndpoint;
        String sessionName = "";
        while (sessionName.length() == 0) {
            final ResponseEntity<LivySessionAvailabilityResponse> responseEntity = this.getGetResponse(getUrl);
            if (responseEntity != null && responseEntity.getStatusCodeValue() < 400) {
                final List<LivySessionResponse> sessions = responseEntity.getBody().getSessions();
                final List<LivySessionResponse> idleSessions = sessions.stream()
                        .filter(session -> session.getState().equalsIgnoreCase("idle"))
                        .collect(Collectors.toList());
                if (idleSessions == null || idleSessions.size() == 0) {
                    final ValidationError v = new ValidationError();
                    v.setErrorCode(HttpStatus.BAD_REQUEST);
                    throw v;
                }
                else {
                    final LivySessionResponse availableSession = idleSessions.get(0);
                    sessionName = availableSession.getName();
                }
            }
            else {
                final ValidationError v = new ValidationError();
                v.setErrorCode(HttpStatus.INTERNAL_SERVER_ERROR);
                v.setErrorDescription("Cannot query the dataset");
                throw v;
            }
        }
        return sessionName;
    }

    public ResponseEntity<LivySessionAvailabilityResponse> getGetResponse(final String url) {
        final RestTemplate restTemplate = this.getRestTemplate();
        final HttpEntity<String> entityCredentials = this.getEntityCredentials();
        final ResponseEntity<LivySessionAvailabilityResponse> responseEntity = restTemplate.exchange(url,
                HttpMethod.GET, entityCredentials, LivySessionAvailabilityResponse.class);
        return responseEntity;
    }

    public RestTemplate getRestTemplate() {
        final RestTemplate restTemplate = new RestTemplate();
        final MappingJackson2HttpMessageConverter jsonHttpMessageConverter = new MappingJackson2HttpMessageConverter();
        jsonHttpMessageConverter.getObjectMapper().configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        restTemplate.getMessageConverters().add(jsonHttpMessageConverter);
        return restTemplate;
    }

    public HttpEntity<String> getEntityCredentials() {
        final HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setAccept(Arrays.asList(MediaType.APPLICATION_JSON));
        final HttpEntity<String> entityCredentials = new HttpEntity<String>(headers);
        return entityCredentials;

    }

    public HttpEntity<String> postEntityCredentials(final String code) {
        final HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setAccept(Arrays.asList(MediaType.APPLICATION_JSON));
        final JSONObject jsonCredentials = new JSONObject();
        jsonCredentials.put("code", code);
        final HttpEntity<String> entityCredentials = new HttpEntity<String>(jsonCredentials.toString(), headers);
        return entityCredentials;
    }

    private LivyPostResponse pollForResult(final ResponseEntity<LivyPostResponse> responseEntity,
            final String availableSession, final String livyEndPoint, final long livyPort,
            final RestTemplate restTemplate) {
        final String jobId = String.valueOf(responseEntity.getBody().getId());
        final String endpoint = "/sessions/" + availableSession + "/statements";
        final String getUrl = this.protocol + livyEndPoint + ":" + livyPort + endpoint + "/" + jobId;
        LivyPostResponse result = restTemplate.getForObject(getUrl, LivyPostResponse.class);
        while (result != null && !result.getState().equals("available")) {
            result = restTemplate.getForObject(getUrl, LivyPostResponse.class);
        }

        return result;
    }

    public List<List<String>> getSampleData(final String datasetName, final String livyEndPoint, final long livyPort,
            final String availableSession) throws ValidationError {
        final String currentEndpoint = this.statementEndpoint.replace("$sessionName$", availableSession);
        final String postUrl = this.protocol + livyEndPoint + ":" + livyPort + currentEndpoint;
        final String code = "com.paypal.gimel.sql.GimelQueryProcessor.executeBatch(\"select * from "
                + "udc." + datasetName.trim()
                + " limit 5\",spark).show(5,false)";

        final RestTemplate restTemplate = this.getRestTemplate();
        final HttpEntity<String> entityCredentials = this.postEntityCredentials(code);

        final ResponseEntity<LivyPostResponse> responseEntity = restTemplate.exchange(postUrl, HttpMethod.POST,
                entityCredentials, LivyPostResponse.class);

        if (responseEntity != null && responseEntity.getStatusCodeValue() < 400) {
            final LivyPostResponse result = this.pollForResult(responseEntity, availableSession, livyEndPoint,
                    livyPort, restTemplate);
            return this.iterateResult(result);

        }
        else {
            final ValidationError v = new ValidationError();
            v.setErrorCode(HttpStatus.INTERNAL_SERVER_ERROR);
            v.setErrorDescription("Cannot query the dataset");
            throw v;
        }
    }

    private List<List<String>> iterateResult(final LivyPostResponse result) throws ValidationError {
        if (result.getOutput().getStatus().equalsIgnoreCase("ok")) {
            final String outputWithConsole = result.getOutput().getData().getData();
            final List<String> output = Arrays
                    .asList(outputWithConsole.substring(outputWithConsole.indexOf("|")).split("\n"));
            final List<List<String>> outputs = new ArrayList<List<String>>();
            output.stream().filter(a -> a.contains("|")).forEach(a -> {
                final List<String> eachRow = Arrays.asList(a.split("\\|"));
                outputs.add(eachRow.stream().filter(a1 -> a1 == null || a1.length() > 0)
                        .map(a1 -> a1.trim())
                        .collect(Collectors.toList()));
            });
            return outputs;
        }
        else {
            final List<String> errorMsgs = result.getOutput().getTraceback().stream().map(error -> error.trim())
                    .collect(Collectors.toList());
            final ValidationError verror = new ValidationError();
            verror.setErrorCode(HttpStatus.BAD_REQUEST);
            verror.setErrorDescription(String.join(",", errorMsgs));
            throw verror;
        }
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

}
