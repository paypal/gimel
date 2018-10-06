package com.paypal.udc.util;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import com.paypal.udc.cache.StorageTypeCache;
import com.paypal.udc.dao.StorageRepository;
import com.paypal.udc.dao.objectschema.ObjectSchemaAttributeValueRepository;
import com.paypal.udc.dao.objectschema.ObjectSchemaMapRepository;
import com.paypal.udc.dao.storagesystem.StorageSystemAttributeValueRepository;
import com.paypal.udc.dao.storagesystem.StorageSystemRepository;
import com.paypal.udc.dao.storagetype.StorageTypeAttributeKeyRepository;
import com.paypal.udc.dao.storagetype.StorageTypeRepository;
import com.paypal.udc.entity.Storage;
import com.paypal.udc.entity.objectschema.ObjectAttributeValue;
import com.paypal.udc.entity.objectschema.ObjectSchemaMap;
import com.paypal.udc.entity.storagesystem.StorageSystemAttributeValue;
import com.paypal.udc.entity.storagetype.CollectiveStorageTypeAttributeKey;
import com.paypal.udc.entity.storagetype.StorageType;
import com.paypal.udc.entity.storagetype.StorageTypeAttributeKey;
import com.paypal.udc.exception.ValidationError;
import com.paypal.udc.util.enumeration.ActiveEnumeration;


@Component
public class StorageTypeUtil {

    @Autowired
    private StorageRepository storageRepository;

    @Autowired
    private StorageTypeRepository storageTypeRepository;

    @Autowired
    private StorageTypeAttributeKeyRepository storageTypeAttributeRepository;

    @Autowired
    private ObjectSchemaMapRepository objectSchemaMapRepository;

    @Autowired
    private StorageSystemRepository storageSystemRepository;

    @Autowired
    private StorageTypeCache storageTypeCache;

    @Autowired
    private ObjectSchemaAttributeValueRepository osavr;

    @Autowired
    private StorageSystemAttributeValueRepository ssavr;

    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss");

    public Map<Long, Storage> getStorages() {
        final Map<Long, Storage> storages = new HashMap<Long, Storage>();
        this.storageRepository.findAll().forEach(
                storage -> storages.put(storage.getStorageId(), storage));
        return storages;
    }

    public void validateStorageTypeId(final long id) throws ValidationError {
        final StorageType storageType = this.storageTypeCache.getStorageType(id);
        if (storageType == null) {
            final ValidationError verror = new ValidationError();
            verror.setErrorCode(HttpStatus.BAD_REQUEST);
            verror.setErrorDescription("Invalid Storage Type");
            throw verror;
        }
    }

    public Map<Long, StorageType> getStorageTypes() {
        final Map<Long, StorageType> storageTypeMap = new HashMap<Long, StorageType>();
        final Map<Long, Storage> storageMap = this.getStorages();
        this.storageTypeRepository.findAll()
                .forEach(storageType -> {
                    final Storage storage = storageMap.get(storageType.getStorageId());
                    storageType.setStorage(storage);
                    storageTypeMap.put(storageType.getStorageTypeId(), storageType);
                });
        return storageTypeMap;
    }

    public Map<Long, StorageTypeAttributeKey> getStorageTypeAttributes() {

        final Map<Long, StorageTypeAttributeKey> storageTypeAttributeMap = new HashMap<Long, StorageTypeAttributeKey>();
        this.storageTypeAttributeRepository.findAll().forEach(storageAttribute -> storageTypeAttributeMap
                .put(storageAttribute.getStorageDsAttributeKeyId(), storageAttribute));

        return storageTypeAttributeMap;
    }

    public void validateAttributes(final CollectiveStorageTypeAttributeKey attributeKey) throws ValidationError {
        final String storageDsAttributeName = attributeKey.getStorageDsAttributeKeyName();
        if (storageDsAttributeName == null || storageDsAttributeName.length() == 0) {
            final ValidationError verror = new ValidationError();
            verror.setErrorCode(HttpStatus.BAD_REQUEST);
            verror.setErrorDescription("Invalid Storage Type Attribute Name");
            throw verror;
        }
        final String storageDsAttributeDescription = attributeKey.getStorageDsAttributeKeyDesc();
        if (storageDsAttributeDescription == null || storageDsAttributeDescription.length() == 0) {
            final ValidationError verror = new ValidationError();
            verror.setErrorCode(HttpStatus.BAD_REQUEST);
            verror.setErrorDescription("Invalid Storage Type Attribute Description");
            throw verror;
        }
    }

    public void insertAttributesForExistingObjects(final StorageTypeAttributeKey stak) {

        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final String time = sdf.format(timestamp);

        final List<Long> storageSystemIds = this.storageSystemRepository
                .findByStorageTypeId(stak.getStorageTypeId())
                .stream().map(stakr -> stakr.getStorageSystemId()).collect(Collectors.toList());
        if (stak.getIsStorageSystemLevel().equals(ActiveEnumeration.YES.getFlag())) {
            if (storageSystemIds != null && storageSystemIds.size() > 0) {
                final List<StorageSystemAttributeValue> ssavs = new ArrayList<StorageSystemAttributeValue>();
                storageSystemIds.forEach(storageSystemId -> {
                    final StorageSystemAttributeValue ssav = new StorageSystemAttributeValue();
                    ssav.setCreatedTimestamp(time);
                    ssav.setCreatedUser(stak.getCreatedUser());
                    ssav.setUpdatedUser(stak.getUpdatedUser());
                    ssav.setUpdatedTimestamp(time);
                    ssav.setIsActiveYN(ActiveEnumeration.YES.getFlag());
                    ssav.setStorageSystemID(storageSystemId);
                    ssav.setStorageDataSetAttributeKeyId(stak.getStorageDsAttributeKeyId());
                    ssav.setStorageSystemAttributeValue("Defaults");
                    ssavs.add(ssav);
                });
                this.ssavr.save(ssavs);
            }
        }
        if (stak.getIsStorageSystemLevel().equals(ActiveEnumeration.NO.getFlag())) {
            final List<ObjectSchemaMap> objects = this.objectSchemaMapRepository
                    .findByStorageSystemIdIn(storageSystemIds);
            if (objects != null && objects.size() > 0) {
                final List<Long> objectIds = objects.stream().map(object -> object.getObjectId())
                        .collect(Collectors.toList());
                final List<ObjectAttributeValue> oavs = new ArrayList<ObjectAttributeValue>();
                objectIds.forEach(objectId -> {
                    final ObjectAttributeValue oav = new ObjectAttributeValue();
                    oav.setObjectId(objectId);
                    oav.setObjectAttributeValue("Defaults");
                    oav.setStorageDsAttributeKeyId(stak.getStorageDsAttributeKeyId());
                    oav.setIsCustomized(ActiveEnumeration.NO.getFlag());
                    oav.setIsActiveYN(ActiveEnumeration.YES.getFlag());
                    oav.setCreatedTimestamp(time);
                    oav.setUpdatedTimestamp(time);
                    oav.setCreatedUser(stak.getCreatedUser());
                    oav.setUpdatedUser(stak.getUpdatedUser());
                    oavs.add(oav);
                });
                this.osavr.save(oavs);
            }
        }

    }

    public void updateAttributeKeysForObjects(final List<StorageTypeAttributeKey> attributeKeys) {

        final List<Long> storageSystemIds = this.storageSystemRepository
                .findByStorageTypeId(attributeKeys.get(0).getStorageTypeId())
                .stream().map(stakr -> stakr.getStorageSystemId()).collect(Collectors.toList());
        final List<ObjectSchemaMap> objects = this.objectSchemaMapRepository
                .findByStorageSystemIdIn(storageSystemIds);

        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final String time = sdf.format(timestamp);

        final List<StorageTypeAttributeKey> systemAttributeKeys = attributeKeys.stream()
                .filter(key -> key.getIsStorageSystemLevel().equals(ActiveEnumeration.YES.getFlag()))
                .collect(Collectors.toList());

        if (systemAttributeKeys != null && systemAttributeKeys.size() > 0) {
            systemAttributeKeys.forEach(systemAttributeKey -> {
                final List<StorageSystemAttributeValue> ssavs = new ArrayList<StorageSystemAttributeValue>();
                storageSystemIds.forEach(storageSystemId -> {
                    final StorageSystemAttributeValue ssav = this.ssavr
                            .findByStorageSystemIdAndStorageDataSetAttributeKeyId(storageSystemId,
                                    systemAttributeKey.getStorageDsAttributeKeyId());
                    ssav.setIsActiveYN(systemAttributeKey.getIsActiveYN());
                    ssav.setUpdatedUser(systemAttributeKey.getUpdatedUser());
                    ssav.setUpdatedTimestamp(time);
                    ssavs.add(ssav);
                });
                this.ssavr.save(ssavs);
            });
        }

        final List<StorageTypeAttributeKey> objectAttributeKeys = attributeKeys.stream()
                .filter(key -> key.getIsStorageSystemLevel().equals(ActiveEnumeration.NO.getFlag()))
                .collect(Collectors.toList());
        final List<Long> objectIds = objects.stream().map(object -> object.getObjectId())
                .collect(Collectors.toList());
        if (objectAttributeKeys != null && objectAttributeKeys.size() > 0) {
            objectAttributeKeys.forEach(objectAttributeKey -> {
                final List<ObjectAttributeValue> oavs = new ArrayList<ObjectAttributeValue>();
                objectIds.forEach(objectId -> {
                    final ObjectAttributeValue oav = this.osavr.findByObjectIdAndStorageDsAttributeKeyId(
                            objectId, objectAttributeKey.getStorageDsAttributeKeyId());
                    oav.setIsActiveYN(objectAttributeKey.getIsActiveYN());
                    oav.setUpdatedUser(objectAttributeKey.getUpdatedUser());
                    oav.setUpdatedTimestamp(time);
                    oavs.add(oav);
                });
                this.osavr.save(oavs);
            });
        }
    }
}
