package com.paypal.udc.service.impl;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.validation.ConstraintViolationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.paypal.udc.cache.DatasetCache;
import com.paypal.udc.cache.ObjectSchemaMapCache;
import com.paypal.udc.dao.ClusterRepository;
import com.paypal.udc.dao.dataset.DatasetChangeLogRegisteredRepository;
import com.paypal.udc.dao.dataset.DatasetRepository;
import com.paypal.udc.dao.dataset.DatasetStorageSystemRepository;
import com.paypal.udc.dao.objectschema.ObjectSchemaAttributeValueRepository;
import com.paypal.udc.dao.objectschema.ObjectSchemaMapRepository;
//import com.paypal.gimel.dao.objectschema.ObjectSchemaRegistrationMapRepository;
import com.paypal.udc.dao.storagesystem.StorageSystemAttributeValueRepository;
import com.paypal.udc.dao.storagesystem.StorageSystemRepository;
import com.paypal.udc.dao.storagetype.StorageTypeRepository;
import com.paypal.udc.entity.Cluster;
import com.paypal.udc.entity.Zone;
import com.paypal.udc.entity.dataset.CumulativeDataset;
import com.paypal.udc.entity.dataset.Dataset;
import com.paypal.udc.entity.dataset.DatasetChangeLogRegistered;
import com.paypal.udc.entity.dataset.DatasetStorageSystem;
import com.paypal.udc.entity.dataset.DatasetWithAttributes;
import com.paypal.udc.entity.objectschema.ObjectAttributeValue;
import com.paypal.udc.entity.objectschema.ObjectSchemaMap;
//import com.paypal.gimel.entity.objectschema.ObjectSchemaRegistrationMap;
import com.paypal.udc.entity.objectschema.Schema;
import com.paypal.udc.entity.storagesystem.StorageSystem;
import com.paypal.udc.entity.storagesystem.StorageSystemAttributeValue;
import com.paypal.udc.entity.storagetype.StorageType;
import com.paypal.udc.entity.storagetype.StorageTypeAttributeKey;
import com.paypal.udc.exception.ValidationError;
import com.paypal.udc.service.IDatasetService;
import com.paypal.udc.util.ClusterUtil;
import com.paypal.udc.util.DatasetUtil;
import com.paypal.udc.util.ObjectSchemaMapUtil;
import com.paypal.udc.util.StorageSystemUtil;
import com.paypal.udc.util.StorageTypeUtil;
import com.paypal.udc.util.UserUtil;
import com.paypal.udc.util.ZoneUtil;
import com.paypal.udc.util.enumeration.ActiveEnumeration;
import com.paypal.udc.util.enumeration.ChangeEnumration;
import com.paypal.udc.validator.dataset.DatasetAliasValidator;
import com.paypal.udc.validator.dataset.DatasetDescValidator;
import com.paypal.udc.validator.dataset.DatasetNameValidator;


@Service
@Transactional(propagation = Propagation.SUPPORTS, readOnly = true)
public class DatasetService implements IDatasetService {

    final static Logger logger = LoggerFactory.getLogger(DatasetService.class);
    final String failureStatus = "FAILED";

    @Autowired
    private StorageSystemUtil systemUtil;

    @Autowired
    private DatasetRepository dataSetRepository;

    @Autowired
    private UserUtil userUtil;

    @Autowired
    private ClusterUtil clusterUtil;

    @Autowired
    private StorageSystemAttributeValueRepository systemAttributeValueRepository;

    @Autowired
    private DatasetStorageSystemRepository datasetStorageSystemRepository;

    @Autowired
    private StorageSystemRepository storageSystemRepository;

    @Autowired
    private DatasetUtil dataSetUtil;

    @Autowired
    private StorageTypeUtil storageTypeUtil;

    @Autowired
    private ZoneUtil zoneUtil;

    @Autowired
    private StorageTypeRepository storageTypeRepository;

    @Autowired
    private DatasetCache dataSetCache;

    @Autowired
    private DatasetNameValidator s1;

    @Autowired
    private DatasetDescValidator s2;

    @Autowired
    private DatasetAliasValidator s4;

    @Autowired
    private DatasetChangeLogRegisteredRepository changeLogRegisteredRepository;

    @Autowired
    private ObjectSchemaMapRepository objectSchemaMapRepository;

    @Autowired
    private ObjectSchemaMapCache objectSchemaMapCache;

    @Autowired
    private ObjectSchemaAttributeValueRepository objectSchemaAttributeRepository;

    @Autowired
    private ClusterRepository clusterRepository;

    @Autowired
    private ObjectSchemaMapUtil schemaMapUtil;

    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss");

    @Override
    public List<CumulativeDataset> getAllDetailedDatasets(final String dataSetSubString) {
        final Map<Long, StorageSystem> storageSystemMappings = this.systemUtil.getStorageSystems();
        // final Map<Long, StorageType> storageTypeMappings = this.storageTypeUtil.getStorageTypes();
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
                // final String storageTypeName = storageTypeMappings.get(storageSystem.getStorageTypeId())
                // .getStorageTypeName();
                final long zoneId = storageSystem.getZoneId();
                // final ObjectSchemaMap objectSchemaMap = this.objectSchemaMapCache
                // .getObject(dataset.getObjectSchemaMapId());
                dataset.setStorageSystemName(storageSystem.getStorageSystemName());
                dataset.setIsGimelCompatible(storageSystem.getIsGimelCompatible());
                dataset.setIsReadCompatible(storageSystem.getIsReadCompatible());
                dataset.setZoneName(zones.get(zoneId).getZoneName());
                // this.dataSetUtil.setAccessControlForDataset(storageTypeName, storageSystem, objectSchemaMap,
                // dataset);
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

        datasetObj.forEach(obj -> {
            final long storageSystemId = ((Number) obj[10]).longValue();
            final long objectSchemaMapId = ((Number) obj[2]).longValue();
            final DatasetWithAttributes dwa = new DatasetWithAttributes(
                    ((Number) obj[0]).longValue(), String.valueOf(obj[1]),
                    objectSchemaMapId, String.valueOf(obj[3]),
                    String.valueOf(obj[4]), String.valueOf(obj[5]),
                    String.valueOf(obj[6]), String.valueOf(obj[7]),
                    String.valueOf(obj[8]), storageSystemId,
                    String.valueOf(obj[11]), String.valueOf(obj[9]), String.valueOf(obj[13]), "", "");

            final ObjectSchemaMap schemaMap = this.objectSchemaMapCache.getObject(objectSchemaMapId);
            final List<Schema> objectSchema = gson.fromJson(schemaMap.getObjectSchemaInString(),
                    new TypeToken<List<Schema>>() {
                    }.getType());
            dwa.setObjectSchema(objectSchema);
            dwa.setCreatedTimestampOnStore(schemaMap.getCreatedTimestampOnStore());
            dwa.setCreatedUserOnStore(schemaMap.getCreatedUserOnStore());

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
            datasets.add(dwa);
        });
        return datasets != null && datasets.size() == 1 ? datasets.get(0) : null;
    }

    @Override
    public Page<Dataset> getAllDatasetsByTypeAndSystem(final String datasetSubstring, final String storageTypeName,
            final String storageSystemName, final Pageable pageable) {

        if (!storageTypeName.equals("All")) {
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
            final String storageTypeName, final String storageSystemName, final Pageable pageable) {

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
            final String storageSystemName, final Pageable pageable) {

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
    public Dataset getDataSetById(final long dataSetId) {
        Dataset dataSet = this.dataSetRepository.findOne(dataSetId);
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
            ConstraintViolationException.class, DataIntegrityViolationException.class })
    public Dataset addDataset(final Dataset dataset) throws ValidationError {

        Dataset tempDataset = new Dataset();
        final String databaseName = "udc";
        // final String hiveType = "Hive";
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
            this.userUtil.validateUser(dataset.getUserId());
            this.clusterUtil.validateClusters(dataset.getClusters());
            final ObjectSchemaMap objectSchemaMap = this.schemaMapUtil
                    .validateSystemContainerObjectMapping(storageSystemId, containerName, objectName);
            final long objectSchemaMapId = objectSchemaMap.getObjectId();
            dataset.setObjectSchemaMapId(objectSchemaMapId);

            // if (storageTypeName.equalsIgnoreCase("Hive") || storageTypeName.equalsIgnoreCase("Hbase")) {
            if (storageTypeName.equalsIgnoreCase("Hbase")) {
                final StorageSystem storageSystem = this.storageSystemRepository.findOne(storageSystemId);
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

        // insert into pc_storage_dataset_change_log_registered table
        // if (!storageTypeName.equals(hiveType) && areAttributesPresent) {
        if (areAttributesPresent) {
            final List<DatasetChangeLogRegistered> changeLogs = new ArrayList<DatasetChangeLogRegistered>();
            final ObjectSchemaMap objectSchemaMap = this.objectSchemaMapRepository
                    .findByStorageSystemIdAndContainerNameAndObjectName(storageSystemId, containerName, objectName);
            tempDataset.setStorageSystemId(storageSystemId);

            final List<Long> clusters = dataset.getClusters();

            for (final long clusterId : clusters) {
                final DatasetChangeLogRegistered changeLog = this.dataSetUtil.populateDSChangeLog(createdUser, time,
                        tempDataset, ChangeEnumration.CREATE.getFlag(), clusterId);
                if (objectSchemaMap.getQuery() != null && objectSchemaMap.getQuery().length() > 0) {
                    final String query = objectSchemaMap.getQuery()
                            .replace("TABLENAME", dataset.getStorageDataSetName())
                            .replace("DATABASE", databaseName);
                    changeLog.setStorageDatasetQuery(query);
                }
                else {
                    changeLog.setStorageDatasetQuery("");
                }
                changeLogs.add(changeLog);
            }
            this.changeLogRegisteredRepository.save(changeLogs);
        }

        // update the object_schema_cluster_map table if its via Auto registration
        if (isAutoRegistered.equals(ActiveEnumeration.YES.getFlag())) {
            this.schemaMapUtil.updateObjectAutoRegistrationStatus(dataset.getObjectSchemaMapId(), areAttributesPresent,
                    storageTypeName);
        }
        return tempDataset;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED, readOnly = false, rollbackFor = {
            TransactionSystemException.class, DataIntegrityViolationException.class, IndexOutOfBoundsException.class,
            ValidationError.class })
    public Dataset updateDataSet(final Dataset dataSet) throws ValidationError {

        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final ValidationError v = new ValidationError();
        final long dataSetId = dataSet.getStorageDataSetId();
        final String time = sdf.format(timestamp);
        final String createdUser = dataSet.getCreatedUser();
        Dataset tempDataSet = this.dataSetCache.getDataSet(dataSetId);
        if (tempDataSet == null) {
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("Dataset ID is invalid");
            throw v;
        }
        try {
            tempDataSet.setUpdatedUser(createdUser);
            tempDataSet.setUpdatedTimestamp(time);
            this.s1.setNextChain(this.s2);
            this.s2.setNextChain(this.s4);
            this.s1.validate(dataSet, tempDataSet);
            tempDataSet = this.dataSetRepository.save(tempDataSet);
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

        // insert into pc_storage_dataset_change_log table
        final List<Long> clusters = this.clusterUtil.getAllClusters().stream().map(cluster -> cluster.getClusterId())
                .collect(Collectors.toList());
        for (final long clusterId : clusters) {
            final DatasetChangeLogRegistered changeLog = this.dataSetUtil.populateDSChangeLog(createdUser, time,
                    tempDataSet, ChangeEnumration.MODIFY.getFlag(), clusterId);

            final long objectSchemaMapId = tempDataSet.getObjectSchemaMapId();
            final ObjectSchemaMap schemaMap = this.objectSchemaMapRepository.findOne(objectSchemaMapId);
            final String query = schemaMap.getQuery().replace("TABLENAME", tempDataSet.getStorageDataSetName())
                    .replace("DATABASE", tempDataSet.getStorageDatabaseName());
            changeLog.setStorageDatasetQuery(query);
            this.changeLogRegisteredRepository.save(changeLog);
        }
        return tempDataSet;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED, readOnly = false, rollbackFor = {
            TransactionSystemException.class, DataIntegrityViolationException.class,
            ValidationError.class })
    public Dataset deleteDataSet(final long dataSetId) throws ValidationError {

        final ValidationError v = new ValidationError();
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());

        // update pc_storage_dataset table
        final Dataset dataSet = this.dataSetCache.getDataSet(dataSetId);
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

        final List<DatasetChangeLogRegistered> changeLogs = this.changeLogRegisteredRepository
                .findByStorageDataSetId(dataSetId);

        changeLogs.forEach(changeLog -> {
            changeLog.setUpdatedTimestamp(sdf.format(timestamp));
            changeLog.setStorageDatasetChangeType(ChangeEnumration.DELETE.getFlag());
            this.changeLogRegisteredRepository.save(changeLog);
        });

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
        final ObjectSchemaMap schemaMap = this.objectSchemaMapCache.getObject(objectSchemaMapId);
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
        dataset.setCreatedTimestampOnStore(schemaMap.getCreatedTimestampOnStore());
        dataset.setCreatedUserOnStore(schemaMap.getCreatedUserOnStore());
        dataset.setQuery(schemaMap.getQuery());
        final String schema = schemaMap.getObjectSchemaInString();
        final List<Schema> modifiedSchema = gson.fromJson(schema, new TypeToken<List<Schema>>() {
        }.getType());
        dataset.setObjectSchema(modifiedSchema);

        // final List<ClusterDeploymentStatus> clusterNames = this.schemaMapUtil
        // .getClustersAndStatusByObjectId(objectSchemaMapId);
        // clusterNames.forEach(clusterName -> {
        // clusterName.setClusterName(clusterMap.get(clusterName.getClusterId()).getClusterName());
        // });
        // dataset.setClusterNames(clusterNames);
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

        return dataset;

    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED, readOnly = false, rollbackFor = { ValidationError.class })
    public void updateDatasetWithPendingAttributes(final DatasetWithAttributes dataset) throws ValidationError {

        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final String time = sdf.format(timestamp);
        final ValidationError v = new ValidationError();
        final Dataset retreivedDataset = this.dataSetCache.getDataSet(dataset.getStorageDataSetId());
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
            // final ObjectSchemaRegistrationMap schemaClusterMap = this.objectSchemaRegistrationRepository
            // .findByObjectId(retreivedDataset.getObjectSchemaMapId());
            // schemaClusterMap.setIsRegistered(ActiveEnumeration.YES.getFlag());
            // schemaClusterMap.setUpdatedTimestamp(time);
            // this.objectSchemaRegistrationRepository.save(schemaClusterMap);
            final ObjectSchemaMap schemaMap = this.objectSchemaMapRepository
                    .findOne(retreivedDataset.getObjectSchemaMapId());
            schemaMap.setIsRegistered(ActiveEnumeration.YES.getFlag());
            schemaMap.setUpdatedTimestamp(time);
            this.objectSchemaMapRepository.save(schemaMap);
        }

        // insert into pc_storage_change_log_registered table
        // final List<Long> clusters = this.objectSchemaClusterRepository.findByObjectId(objectId).stream()
        // .map(schemaCluster -> schemaCluster.getClusterId()).collect(Collectors.toList());
        final List<Long> clusters = this.clusterUtil.getAllClusters().stream().map(cluster -> cluster.getClusterId())
                .collect(Collectors.toList());
        clusters.forEach(clusterId -> {
            final DatasetChangeLogRegistered changeLog = new DatasetChangeLogRegistered();
            changeLog.setStorageDataSetId(retreivedDataset.getStorageDataSetId());
            changeLog.setStorageDataSetName(retreivedDataset.getStorageDataSetName());
            changeLog.setStorageDataSetAliasName(retreivedDataset.getStorageDataSetAliasName() + "_");
            changeLog.setStorageContainerName(retreivedDataset.getStorageContainerName());
            changeLog.setStorageDatabaseName(retreivedDataset.getStorageDatabaseName());
            changeLog.setStorageDataSetDescription(retreivedDataset.getStorageDataSetDescription());
            changeLog.setStorageSystemId(dataset.getStorageSystemId());
            changeLog.setStorageClusterId(clusterId);
            changeLog.setUserId(retreivedDataset.getUserId());
            changeLog.setStorageDatasetChangeType(ChangeEnumration.CREATE.getFlag());
            changeLog.setCreatedTimestamp(time);
            changeLog.setUpdatedTimestamp(time);
            final ObjectSchemaMap schemaMap = this.objectSchemaMapRepository.findOne(dataset.getObjectSchemaMapId());
            final String query = schemaMap.getQuery().replace("TABLENAME", retreivedDataset.getStorageDataSetName())
                    .replace("DATABASE", "udc");
            changeLog.setStorageDatasetQuery(query);
            changeLog.setCreatedUser(retreivedDataset.getCreatedUser());
            changeLog.setUpdatedUser(retreivedDataset.getUpdatedUser());
            this.changeLogRegisteredRepository.save(changeLog);
        });
    }

    @Override
    public List<DatasetChangeLogRegistered> getDatasetChangeLogs(final long storageClusterId) {
        final List<DatasetChangeLogRegistered> changeLogs = this.changeLogRegisteredRepository
                .findChanges(storageClusterId);
        return changeLogs;
    }

    @Override
    public long getDatasetCount() {
        final long datasetCount = this.dataSetRepository.count();
        return datasetCount;
    }

    @Override
    public List<List<String>> getSampleData(final String datasetName, final long objectId) throws ValidationError {
        final Gson gson = new Gson();
        final ObjectSchemaMap object = this.objectSchemaMapCache.getObject(objectId);
        final List<Schema> objectSchema = gson.fromJson(object.getObjectSchemaInString(),
                new TypeToken<List<Schema>>() {
                }.getType());
        final StorageSystem storageSystem = this.storageSystemRepository.findOne(object.getStorageSystemId());
        if (storageSystem.getIsGimelCompatible().equals(ActiveEnumeration.YES.getFlag())) {
            final Cluster cluster = this.clusterUtil.getClusterByObjectId(objectId);
            final String livyEndPoint = cluster.getLivyEndPoint();
            final long livyPort = cluster.getLivyPort();
            final String availableSession = this.dataSetUtil.getSessionAvailability(livyEndPoint, livyPort);
            final List<List<String>> output = this.dataSetUtil.getSampleData(datasetName, livyEndPoint, livyPort,
                    availableSession);
            if (output != null && output.size() > 0) {
                final List<List<String>> maskedOutput = this.dataSetUtil.getMaskedData(output, objectSchema);
                return maskedOutput;
            }
            else {
                return output;
            }

        }
        else {
            return new ArrayList<List<String>>();
        }

    }
}
