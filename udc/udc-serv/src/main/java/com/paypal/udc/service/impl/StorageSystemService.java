package com.paypal.udc.service.impl;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.validation.ConstraintViolationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import com.paypal.udc.cache.StorageSystemCache;
import com.paypal.udc.cache.StorageTypeCache;
import com.paypal.udc.dao.storagesystem.StorageSystemAttributeValueRepository;
import com.paypal.udc.dao.storagesystem.StorageSystemContainerRepository;
import com.paypal.udc.dao.storagesystem.StorageSystemRepository;
import com.paypal.udc.dao.storagetype.StorageTypeAttributeKeyRepository;
import com.paypal.udc.dao.storagetype.StorageTypeRepository;
import com.paypal.udc.entity.Zone;
import com.paypal.udc.entity.storagesystem.StorageSystem;
import com.paypal.udc.entity.storagesystem.StorageSystemAttributeValue;
import com.paypal.udc.entity.storagesystem.StorageSystemContainer;
import com.paypal.udc.entity.storagetype.StorageType;
import com.paypal.udc.entity.storagetype.StorageTypeAttributeKey;
import com.paypal.udc.exception.ValidationError;
import com.paypal.udc.service.IStorageSystemService;
import com.paypal.udc.util.ClusterUtil;
import com.paypal.udc.util.StorageSystemUtil;
import com.paypal.udc.util.StorageTypeUtil;
import com.paypal.udc.util.UserUtil;
import com.paypal.udc.util.ZoneUtil;
import com.paypal.udc.util.enumeration.ActiveEnumeration;
import com.paypal.udc.validator.storagesystem.StorageSystemDescValidator;
import com.paypal.udc.validator.storagesystem.StorageSystemNameValidator;
import com.paypal.udc.validator.storagesystem.StorageSystemTypeIDValidator;


@Service
@Transactional(propagation = Propagation.SUPPORTS, readOnly = true)
public class StorageSystemService implements IStorageSystemService {

    @Autowired
    private StorageSystemRepository storageSystemRepository;

    @Autowired
    private StorageSystemContainerRepository storageSystemContainerRepository;

    @Autowired
    private StorageTypeCache storageTypeCache;

    @Autowired
    private ClusterUtil clusterUtil;

    @Autowired
    private ZoneUtil zoneUtil;

    @Autowired
    private StorageTypeRepository storageTypeRepository;

    @Autowired
    private StorageSystemCache storageSystemCache;

    @Autowired
    private StorageSystemAttributeValueRepository systemAttributeValueRepository;

    @Autowired
    private StorageTypeAttributeKeyRepository typeAttributeKeyRepository;

    @Autowired
    private UserUtil userUtil;

    @Autowired
    private StorageSystemContainerRepository systemContainerRepository;

    @Autowired
    private StorageTypeUtil storageTypeUtil;

    @Autowired
    private StorageSystemUtil storageSystemUtil;

    @Autowired
    private StorageSystemTypeIDValidator s3;

    @Autowired
    private StorageSystemNameValidator s1;

    @Autowired
    private StorageSystemDescValidator s2;

    @Autowired
    private StorageSystemAttributeValueRepository ssavr;

    final static Logger logger = LoggerFactory.getLogger(StorageSystemService.class);
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss");

    @Override
    public List<StorageSystem> getAllStorageSystems() {

        final Map<Long, Zone> zones = this.zoneUtil.getZones();
        final Map<Long, StorageType> storageTypes = this.storageTypeUtil.getStorageTypes();
        final List<StorageSystem> storageSystems = new ArrayList<StorageSystem>();
        this.storageSystemRepository.findAll().forEach(storageSystem -> {
            if (storageSystem.getIsActiveYN().equals(ActiveEnumeration.YES.getFlag())) {
                final List<StorageSystemContainer> systemContainers = this.systemContainerRepository
                        .findByStorageSystemId(storageSystem.getStorageSystemId());
                final StorageType storageType = storageTypes.get(storageSystem.getStorageTypeId());
                storageSystem.setStorageType(storageType);
                final String containers = systemContainers.stream()
                        .map(systemContainer -> systemContainer.getContainerName()).collect(Collectors.joining(","));
                storageSystem.setContainers(containers.equals(",") ? "" : containers);
                storageSystem.setZoneName(zones.get(storageSystem.getZoneId()).getZoneName());
                storageSystems.add(storageSystem);
            }
        });
        return storageSystems;
    }

    @Override
    public StorageSystem getStorageSystemById(final long storageSystemId) {
        final StorageSystem storageSystem = this.storageSystemRepository.findOne(storageSystemId);
        final StorageType storageType = this.storageTypeCache.getStorageType(storageSystem.getStorageTypeId());
        storageSystem.setStorageType(storageType);
        return storageSystem;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED, readOnly = false, rollbackFor = { ValidationError.class,
            ConstraintViolationException.class, DataIntegrityViolationException.class })
    public StorageSystem addStorageSystem(final StorageSystem storageSystem) throws ValidationError {
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final String time = sdf.format(timestamp);
        StorageSystem insertedStorageSystem = new StorageSystem();
        final String createdUser = storageSystem.getCreatedUser();
        final List<StorageSystemAttributeValue> attributeValues = storageSystem.getSystemAttributeValues();
        final ValidationError v = new ValidationError();
        long storageSystemId;
        // insert into pc_storage_system table
        try {
            this.userUtil.validateUser(createdUser);
            this.clusterUtil.validateCluster(storageSystem.getAssignedClusterId());
            this.clusterUtil.validateCluster(storageSystem.getRunningClusterId());
            this.storageTypeUtil.validateStorageTypeId(storageSystem.getStorageTypeId());
            this.zoneUtil.validateZone(storageSystem.getZoneId());
            storageSystem.setUpdatedUser(createdUser);
            storageSystem.setCreatedTimestamp(time);
            storageSystem.setUpdatedTimestamp(time);
            storageSystem.setIsActiveYN(ActiveEnumeration.YES.getFlag());
            insertedStorageSystem = this.storageSystemRepository.save(storageSystem);
            storageSystemId = insertedStorageSystem.getStorageSystemId();
        }
        catch (final ConstraintViolationException e) {
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("Storage System name is empty");
            throw v;
        }
        catch (final DataIntegrityViolationException e) {
            v.setErrorCode(HttpStatus.CONFLICT);
            v.setErrorDescription(
                    "Storage System name is duplicated or Invalid Cluster ID or Invalid zone id or Invalid user Id");
            throw v;
        }

        // insert into pc_storage_system_attribute_value table
        if (attributeValues != null && attributeValues.size() > 0) {
            try {
                attributeValues.forEach(attributeValue -> {
                    attributeValue.setIsActiveYN(ActiveEnumeration.YES.getFlag());
                    attributeValue.setCreatedTimestamp(time);
                    attributeValue.setUpdatedTimestamp(time);
                    attributeValue.setCreatedUser(createdUser);
                    attributeValue.setUpdatedUser(createdUser);
                    attributeValue.setStorageSystemID(storageSystemId);
                });
                final List<StorageSystemAttributeValue> insertedValues = new ArrayList<StorageSystemAttributeValue>();
                this.ssavr.save(attributeValues).forEach(insertedValues::add);
                insertedStorageSystem.setSystemAttributeValues(insertedValues);
            }
            catch (final ConstraintViolationException e) {
                v.setErrorCode(HttpStatus.BAD_REQUEST);
                v.setErrorDescription("Storage System Attribute value is empty");
                throw v;
            }
            catch (final DataIntegrityViolationException e) {
                v.setErrorCode(HttpStatus.CONFLICT);
                v.setErrorDescription("Invalid Storage Type Attribute Key ID");
                throw v;
            }
        }
        final StorageType storageType = this.storageTypeCache.getStorageType(storageSystem.getStorageTypeId());
        final String storageTypeName = storageType.getStorageTypeName();
        final String storageSystemName = storageSystem.getStorageSystemName();
        final long clusterId = storageTypeName.equalsIgnoreCase("Hbase")
                ? this.storageSystemUtil.getClusterId(storageSystemName)
                : storageSystem.getAssignedClusterId();

        // insert into pc_storage_system_container table
        if (storageSystem.getContainers() == null || storageSystem.getContainers().length() == 0
                || storageSystem.getContainers().equals("All")) {
            final StorageSystemContainer ssc = new StorageSystemContainer();
            ssc.setContainerName(storageSystem.getContainers());
            ssc.setIsActiveYN(ActiveEnumeration.YES.getFlag());
            ssc.setStorageSystemId(storageSystemId);
            ssc.setContainerName("All");
            ssc.setClusterId(clusterId);
            ssc.setUpdatedTimestamp(time);
            ssc.setCreatedTimestamp(time);
            ssc.setCreatedUser(createdUser);
            ssc.setUpdatedUser(createdUser);
            this.storageSystemContainerRepository.save(ssc);
        }
        else {
            final List<StorageSystemContainer> sscs = new ArrayList<StorageSystemContainer>();
            final List<String> containerArray = Arrays.asList(storageSystem.getContainers().split(","));
            containerArray.forEach(container -> {
                final StorageSystemContainer ssc = new StorageSystemContainer();
                ssc.setContainerName(container);
                ssc.setIsActiveYN(ActiveEnumeration.YES.getFlag());
                ssc.setStorageSystemId(storageSystemId);
                ssc.setClusterId(clusterId);
                ssc.setUpdatedTimestamp(time);
                ssc.setCreatedTimestamp(time);
                ssc.setCreatedUser(createdUser);
                ssc.setUpdatedUser(createdUser);
                sscs.add(ssc);
            });
            this.storageSystemContainerRepository.save(sscs);
        }
        return insertedStorageSystem;

    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED, readOnly = false, rollbackFor = { ValidationError.class,
            ConstraintViolationException.class, DataIntegrityViolationException.class,
            TransactionSystemException.class })
    public StorageSystem updateStorageSystem(final StorageSystem storageSystem) throws ValidationError {
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final ValidationError v = new ValidationError();
        StorageSystem tempStorageSystem = this.storageSystemRepository.findOne(storageSystem.getStorageSystemId());
        if (tempStorageSystem != null) {
            try {
                tempStorageSystem.setUpdatedUser(storageSystem.getCreatedUser());
                tempStorageSystem.setUpdatedTimestamp(sdf.format(timestamp));
                this.s1.setNextChain(this.s2);
                this.s2.setNextChain(this.s3);
                this.s1.validate(storageSystem, tempStorageSystem);
                tempStorageSystem.setIsGimelCompatible(storageSystem.getIsGimelCompatible());
                tempStorageSystem.setIsReadCompatible(storageSystem.getIsReadCompatible());
                tempStorageSystem = this.storageSystemRepository.save(tempStorageSystem);
            }
            catch (final TransactionSystemException e) {
                v.setErrorCode(HttpStatus.BAD_REQUEST);
                v.setErrorDescription("Storage System name is empty");
                throw v;
            }
            catch (final DataIntegrityViolationException e) {
                v.setErrorCode(HttpStatus.CONFLICT);
                v.setErrorDescription("Storage System is duplicated");
                throw v;
            }

            final StorageType storageType = this.storageTypeCache
                    .getStorageType(tempStorageSystem.getStorageTypeId());
            final String storageTypeName = storageType.getStorageTypeName();
            final String storageSystemName = storageSystem.getStorageSystemName();
            final List<StorageSystemContainer> systemContainers = this.storageSystemContainerRepository
                    .findByStorageSystemId(storageSystem.getStorageSystemId());
            final long clusterId = storageTypeName.equalsIgnoreCase("Hbase")
                    ? this.storageSystemUtil.getClusterId(storageSystemName)
                    : systemContainers.get(0).getClusterId();

            final List<StorageSystemAttributeValue> attributeValues = storageSystem.getSystemAttributeValues();
            if (attributeValues != null && attributeValues.size() > 0) {
                attributeValues.forEach(attr -> {
                    attr.setUpdatedTimestamp(sdf.format(timestamp));
                    attr.setUpdatedUser(storageSystem.getUpdatedUser());
                    attr.setIsActiveYN(ActiveEnumeration.YES.getFlag());
                    this.systemAttributeValueRepository.save(attr);
                });
            }

            final String containers = storageSystem.getContainers();
            if (containers != null && containers.length() > 0) {
                if (systemContainers != null) {
                    systemContainers.forEach(systemContainer -> {
                        this.storageSystemContainerRepository.delete(systemContainer);
                    });
                }
                final List<StorageSystemContainer> newSystemContainers = new ArrayList<StorageSystemContainer>();
                final List<String> containerList = Arrays.asList(containers.split(","));
                containerList.forEach(container -> {
                    final StorageSystemContainer systemContainer = new StorageSystemContainer();
                    systemContainer.setStorageSystemId(storageSystem.getStorageSystemId());
                    systemContainer.setContainerName(container);
                    systemContainer.setClusterId(clusterId);
                    systemContainer.setIsActiveYN(ActiveEnumeration.YES.getFlag());
                    systemContainer.setCreatedUser(storageSystem.getUpdatedUser());
                    systemContainer.setUpdatedUser(storageSystem.getUpdatedUser());
                    systemContainer.setCreatedTimestamp(sdf.format(timestamp));
                    systemContainer.setUpdatedTimestamp(sdf.format(timestamp));
                    newSystemContainers.add(systemContainer);
                });
                this.storageSystemContainerRepository.save(newSystemContainers);
            }
            return tempStorageSystem;
        }
        else {
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("Storage System ID is invalid");
            throw v;
        }
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED, readOnly = false, rollbackFor = { ValidationError.class,
            ConstraintViolationException.class, DataIntegrityViolationException.class })
    public StorageSystem enableStorageSystem(final long storageSystemId) throws ValidationError {
        final ValidationError v = new ValidationError();
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        StorageSystem storageSystem = this.storageSystemCache.getStorageSystem(storageSystemId);
        if (storageSystem != null) {
            storageSystem.setUpdatedTimestamp(sdf.format(timestamp));
            storageSystem.setIsActiveYN(ActiveEnumeration.YES.getFlag());
            storageSystem = this.storageSystemRepository.save(storageSystem);
            final List<StorageSystemAttributeValue> attributeValues = this.ssavr
                    .findByStorageSystemIdAndIsActiveYN(storageSystemId, ActiveEnumeration.NO.getFlag());
            attributeValues.forEach(attributeValue -> {
                attributeValue.setUpdatedTimestamp(sdf.format(timestamp));
                attributeValue.setIsActiveYN(ActiveEnumeration.YES.getFlag());
            });
            final List<StorageSystemAttributeValue> deactivatedValues = new ArrayList<StorageSystemAttributeValue>();
            this.ssavr.save(attributeValues).forEach(deactivatedValues::add);
            storageSystem.setSystemAttributeValues(deactivatedValues);
            final List<StorageSystemContainer> sscs = this.storageSystemContainerRepository
                    .findByStorageSystemId(storageSystemId);
            sscs.forEach(ssc -> {
                ssc.setIsActiveYN(ActiveEnumeration.YES.getFlag());
                ssc.setUpdatedTimestamp(sdf.format(timestamp));
                this.storageSystemContainerRepository.save(ssc);
            });
            return storageSystem;
        }
        else {
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("Storage System ID is invalid");
            throw v;
        }
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED, readOnly = false, rollbackFor = { ValidationError.class,
            ConstraintViolationException.class, DataIntegrityViolationException.class })
    public StorageSystem deleteStorageSystem(final long storageSystemId) throws ValidationError {
        final ValidationError v = new ValidationError();
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        StorageSystem storageSystem = this.storageSystemCache.getStorageSystem(storageSystemId);
        if (storageSystem != null) {
            storageSystem.setUpdatedTimestamp(sdf.format(timestamp));
            storageSystem.setIsActiveYN(ActiveEnumeration.NO.getFlag());
            storageSystem = this.storageSystemRepository.save(storageSystem);
            final List<StorageSystemAttributeValue> attributeValues = this.ssavr
                    .findByStorageSystemIdAndIsActiveYN(storageSystemId, ActiveEnumeration.YES.getFlag());
            attributeValues.forEach(attributeValue -> {
                attributeValue.setUpdatedTimestamp(sdf.format(timestamp));
                attributeValue.setIsActiveYN(ActiveEnumeration.NO.getFlag());
            });
            final List<StorageSystemAttributeValue> deactivatedValues = new ArrayList<StorageSystemAttributeValue>();
            this.ssavr.save(attributeValues).forEach(deactivatedValues::add);
            storageSystem.setSystemAttributeValues(deactivatedValues);

            final List<StorageSystemContainer> sscs = this.storageSystemContainerRepository
                    .findByStorageSystemId(storageSystemId);
            sscs.forEach(ssc -> {
                ssc.setIsActiveYN(ActiveEnumeration.NO.getFlag());
                ssc.setUpdatedTimestamp(sdf.format(timestamp));
                this.storageSystemContainerRepository.save(ssc);
            });
            return storageSystem;
        }
        else {
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("Storage System ID is invalid");
            throw v;
        }

    }

    @Override
    public List<StorageSystem> getStorageSystemByStorageType(final long storageTypeId) {
        final List<StorageSystem> storageSystems = new ArrayList<StorageSystem>();
        this.storageSystemRepository.findByStorageTypeId(storageTypeId).forEach(
                storageSystem -> storageSystems.add(storageSystem));
        return storageSystems;
    }

    @Override
    public List<StorageSystemAttributeValue> getStorageSystemAttributes(final Long storageSystemId) {
        return this.storageSystemUtil.getAttributes(storageSystemId);
    }

    @Override
    public List<StorageSystemAttributeValue> getAttributeValuesByName(final String storageSystemName) {
        final Map<Long, StorageTypeAttributeKey> typeAttributesMap = this.storageTypeUtil.getStorageTypeAttributes();
        final StorageSystem storageSystem = this.storageSystemRepository.findByStorageSystemName(storageSystemName);
        final List<StorageSystemAttributeValue> systemAttributes = this.systemAttributeValueRepository
                .findByStorageSystemIdAndIsActiveYN(storageSystem.getStorageSystemId(),
                        ActiveEnumeration.YES.getFlag());
        systemAttributes.forEach(sysAttr -> {
            sysAttr.setStorageDsAttributeKeyName(
                    typeAttributesMap.get(sysAttr.getStorageDataSetAttributeKeyId()).getStorageDsAttributeKeyName());
        });

        final StorageType storageType = this.storageTypeCache.getStorageType(storageSystem.getStorageTypeId());
        final List<StorageTypeAttributeKey> typeAttributesAtSystemLevel = this.typeAttributeKeyRepository
                .findByStorageTypeIdAndIsStorageSystemLevelAndIsActiveYN(storageType.getStorageTypeId(),
                        ActiveEnumeration.YES.getFlag(), ActiveEnumeration.YES.getFlag());

        if (typeAttributesAtSystemLevel != null && typeAttributesAtSystemLevel.size() > 0
                && typeAttributesAtSystemLevel.size() == systemAttributes.size()) {
            return systemAttributes;
        }
        else {
            final List<StorageSystemAttributeValue> pendingSystemAttributes = new ArrayList<StorageSystemAttributeValue>();
            final List<Long> systemAttributeIds = systemAttributes.stream()
                    .map(sAttr -> sAttr.getStorageDataSetAttributeKeyId()).collect(Collectors.toList());
            final List<StorageTypeAttributeKey> pendingTypeAttributes = typeAttributesAtSystemLevel.stream()
                    .filter(attr -> !systemAttributeIds.contains(attr.getStorageDsAttributeKeyId()))
                    .collect(Collectors.toList());
            if (pendingTypeAttributes != null && pendingTypeAttributes.size() > 0) {
                pendingTypeAttributes.forEach(attr -> {
                    final StorageSystemAttributeValue ssav = new StorageSystemAttributeValue();
                    ssav.setStorageDataSetAttributeKeyId(attr.getStorageDsAttributeKeyId());
                    ssav.setStorageDsAttributeKeyName(attr.getStorageDsAttributeKeyName());
                    ssav.setStorageSystemID(storageSystem.getStorageSystemId());
                    ssav.setStorageSystemAttributeValue("Defaults");
                    pendingSystemAttributes.add(ssav);
                });
            }
            final Stream<StorageSystemAttributeValue> combinedStream = Stream
                    .of(systemAttributes, pendingSystemAttributes)
                    .flatMap(Collection::stream);
            final List<StorageSystemAttributeValue> collectionCombined = combinedStream
                    .collect(Collectors.toList());
            return collectionCombined;
        }

    }

    @Override
    public List<StorageSystem> getStorageSystemByType(final String storageTypeName) throws ValidationError {

        final Map<Long, Zone> zones = this.zoneUtil.getZones();
        if (storageTypeName.equals("All")) {
            final List<StorageSystem> systems = new ArrayList<StorageSystem>();
            this.storageSystemRepository.findAll().forEach(system -> {
                final List<StorageSystemContainer> systemContainers = this.systemContainerRepository
                        .findByStorageSystemId(system.getStorageSystemId());
                final String containers = systemContainers.stream()
                        .map(systemContainer -> systemContainer.getContainerName()).collect(Collectors.joining(","));
                system.setContainers(containers.equals(",") ? "" : containers);
                system.setZoneName(zones.get(system.getZoneId()).getZoneName());
                systems.add(system);

            });
            return systems;
        }
        else {
            final StorageType storageType = this.storageTypeRepository.findByStorageTypeName(storageTypeName);
            if (storageType == null) {
                final ValidationError v = new ValidationError();
                v.setErrorCode(HttpStatus.BAD_REQUEST);
                v.setErrorDescription("Invalid Storage Type Name");
                throw v;
            }
            final List<StorageSystem> storageSystems = this.storageSystemRepository
                    .findByStorageTypeId(storageType.getStorageTypeId());
            storageSystems.forEach(storageSystem -> {
                final List<StorageSystemContainer> systemContainers = this.systemContainerRepository
                        .findByStorageSystemId(storageSystem.getStorageSystemId());
                final String containers = systemContainers.stream()
                        .map(systemContainer -> systemContainer.getContainerName()).collect(Collectors.joining(","));
                storageSystem.setContainers(containers.equals(",") ? "" : containers);
                storageSystem.setZoneName(zones.get(storageSystem.getZoneId()).getZoneName());
            });

            return storageSystems;
        }

    }

}
