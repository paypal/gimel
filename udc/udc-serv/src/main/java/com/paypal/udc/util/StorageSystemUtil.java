package com.paypal.udc.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import com.paypal.udc.dao.ClusterRepository;
import com.paypal.udc.dao.storagesystem.StorageSystemAttributeValueRepository;
import com.paypal.udc.dao.storagesystem.StorageSystemRepository;
import com.paypal.udc.dao.storagetype.StorageTypeRepository;
import com.paypal.udc.entity.Cluster;
import com.paypal.udc.entity.storagesystem.StorageSystem;
import com.paypal.udc.entity.storagesystem.StorageSystemAttributeValue;
import com.paypal.udc.entity.storagetype.StorageType;
import com.paypal.udc.entity.storagetype.StorageTypeAttributeKey;
import com.paypal.udc.exception.ValidationError;
import com.paypal.udc.service.impl.StorageSystemService;
import com.paypal.udc.util.enumeration.ActiveEnumeration;


@Component
public class StorageSystemUtil {

    final static Logger logger = LoggerFactory.getLogger(StorageSystemService.class);

    @Autowired
    private StorageTypeUtil storageTypeUtil;
    @Autowired
    private StorageSystemRepository storageSystemRepository;
    @Autowired
    private StorageTypeRepository storageTypeRepository;
    @Autowired
    private StorageSystemAttributeValueRepository ssavr;
    @Autowired
    private ClusterRepository clusterRepository;

    public Map<Long, StorageSystem> getStorageSystems() {
        final Map<Long, StorageSystem> storageSystemMap = new HashMap<Long, StorageSystem>();
        this.storageSystemRepository.findAll()
                .forEach(storageSystem -> {
                    storageSystemMap.put(storageSystem.getStorageSystemId(), storageSystem);
                });
        return storageSystemMap;
    }

    public Map<Long, List<StorageSystemAttributeValue>> getStorageSystemAttributes() {
        final Map<Long, List<StorageSystemAttributeValue>> storageAttributeMap = new HashMap<Long, List<StorageSystemAttributeValue>>();
        this.ssavr.findAll().forEach(attributeValue -> {
            final long storageSystemID = attributeValue.getStorageSystemID();
            if (storageAttributeMap.get(storageSystemID) == null) {
                final List<StorageSystemAttributeValue> attributeValues = new ArrayList<StorageSystemAttributeValue>();
                attributeValues.add(attributeValue);
                storageAttributeMap.put(storageSystemID, attributeValues);
            }
            else {
                final List<StorageSystemAttributeValue> attributeValues = storageAttributeMap.get(storageSystemID);
                attributeValues.add(attributeValue);
                storageAttributeMap.put(storageSystemID, attributeValues);
            }
        });
        return storageAttributeMap;
    }

    public void validateStorageSystem(final long storageSystemId) throws ValidationError {
        final StorageSystem storageSystem = this.storageSystemRepository.findOne(storageSystemId);
        if (storageSystem == null) {
            final ValidationError verror = new ValidationError();
            verror.setErrorCode(HttpStatus.BAD_REQUEST);
            verror.setErrorDescription("Invalid Storage System");
            throw verror;
        }
    }

    public StorageType getStorageType(final long storageSystemId) {
        final StorageSystem storageSystem = this.storageSystemRepository.findOne(storageSystemId);
        if (storageSystem != null) {
            final StorageType storageType = this.storageTypeRepository.findOne(storageSystem.getStorageTypeId());
            return storageType;
        }
        return null;
    }

    public StorageSystem getStorageSystem(final String storageSystemName) throws ValidationError {
        final StorageSystem storageSystem = this.storageSystemRepository.findByStorageSystemName(storageSystemName);
        if (storageSystem == null) {
            final ValidationError verror = new ValidationError();
            verror.setErrorCode(HttpStatus.BAD_REQUEST);
            verror.setErrorDescription("Invalid Storage System Name");
            throw verror;
        }
        return storageSystem;
    }

    public List<StorageSystemAttributeValue> getAttributes(final long storageSystemId) {
        final Map<Long, StorageTypeAttributeKey> storageTypeAttributeMap = this.storageTypeUtil
                .getStorageTypeAttributes();
        final List<StorageSystemAttributeValue> attributeValues = this.ssavr.findByStorageSystemIdAndIsActiveYN(
                storageSystemId,
                ActiveEnumeration.YES.getFlag());
        attributeValues.forEach(attributeValue -> {
            final String storageDsAttributeKeyName = storageTypeAttributeMap
                    .get(attributeValue.getStorageDataSetAttributeKeyId())
                    .getStorageDsAttributeKeyName();
            attributeValue.setStorageDsAttributeKeyName(storageDsAttributeKeyName);
        });
        return attributeValues;
    }

    public long getClusterId(final String storageSystemName) throws ValidationError {
        long retrievedClusterId = 0;
        final Map<Long, Cluster> clusterMap = new HashMap<Long, Cluster>();
        this.clusterRepository.findAll().forEach(cluster -> {
            clusterMap.put(cluster.getClusterId(), cluster);
        });
        final List<String> systemCluster = Arrays.asList(storageSystemName.split("\\."));
        if (systemCluster.size() == 2) {
            for (final Entry<Long, Cluster> e : clusterMap.entrySet()) {
                if (e.getValue().getClusterName().toLowerCase().contains(systemCluster.get(1).toLowerCase())) {
                    retrievedClusterId = e.getKey();
                    break;
                }
            }
            if (retrievedClusterId == 0) {
                final ValidationError v = new ValidationError();
                v.setErrorCode(HttpStatus.BAD_REQUEST);
                v.setErrorDescription("Invalid Cluster suffix");
                throw v;
            }
        }
        else {
            final ValidationError v = new ValidationError();
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("Invalid Storage System Format. Please Adhere to StorageSystem.Cluster standard");
            throw v;
        }
        return retrievedClusterId;
    }

    public Map<StorageType, List<StorageSystem>> getTypeToSystemMappings() {
        final Map<StorageType, List<StorageSystem>> typeSystemMappings = new HashMap<StorageType, List<StorageSystem>>();
        final List<StorageType> storageTypes = new ArrayList<StorageType>();
        this.storageTypeRepository.findAll().forEach(storageType -> {
            storageTypes.add(storageType);
        });

        storageTypes.forEach(storageType -> {
            final List<StorageSystem> storageSystems = this.storageSystemRepository
                    .findByStorageTypeId(storageType.getStorageTypeId());
            typeSystemMappings.put(storageType, storageSystems);
        });
        return typeSystemMappings;
    }
}
