package com.paypal.udc.service.impl;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
import com.paypal.udc.cache.StorageCache;
import com.paypal.udc.cache.StorageTypeCache;
import com.paypal.udc.dao.StorageRepository;
import com.paypal.udc.dao.storagetype.StorageTypeAttributeKeyRepository;
import com.paypal.udc.dao.storagetype.StorageTypeRepository;
import com.paypal.udc.entity.Storage;
import com.paypal.udc.entity.storagetype.CollectiveStorageTypeAttributeKey;
import com.paypal.udc.entity.storagetype.StorageType;
import com.paypal.udc.entity.storagetype.StorageTypeAttributeKey;
import com.paypal.udc.exception.ValidationError;
import com.paypal.udc.service.IStorageTypeService;
import com.paypal.udc.util.StorageTypeUtil;
import com.paypal.udc.util.enumeration.ActiveEnumeration;
import com.paypal.udc.validator.storagetype.StorageIDValidator;
import com.paypal.udc.validator.storagetype.StorageTypeDescValidator;
import com.paypal.udc.validator.storagetype.StorageTypeNameValidator;


@Service
@Transactional(propagation = Propagation.SUPPORTS, readOnly = true)
public class StorageTypeService implements IStorageTypeService {

    @Autowired
    private StorageTypeRepository storageTypeRepository;

    @Autowired
    private StorageTypeAttributeKeyRepository storageAttributeRepository;

    @Autowired
    private StorageCache storageCache;

    @Autowired
    private StorageTypeUtil storageTypeUtil;

    @Autowired
    private StorageIDValidator s3;

    @Autowired
    private StorageTypeNameValidator s1;

    @Autowired
    private StorageTypeDescValidator s2;

    @Autowired
    private StorageTypeCache storageTypeCache;

    @Autowired
    private StorageRepository storageRepository;

    final static Logger logger = LoggerFactory.getLogger(StorageTypeService.class);
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss");

    @Override
    public List<StorageType> getAllStorageTypes() {
        final Map<Long, Storage> storages = this.storageTypeUtil.getStorages();
        final List<StorageType> storageTypes = new ArrayList<StorageType>();
        this.storageTypeRepository.findAll().forEach(storageType -> {
            if (storageType.getIsActiveYN().equals(ActiveEnumeration.YES.getFlag())) {
                // final List<StorageTypeAttributeKey> attributeKeys = this.storageAttributeRepository
                // .findByStorageTypeId(storageType.getStorageId());
                storageType.setStorage(storages.get(storageType.getStorageId()));
                // storageType.setAttributeKeys(attributeKeys);
                storageTypes.add(storageType);
            }
        });
        return storageTypes;
    }

    @Override
    public StorageType getStorageTypeById(final long storageTypeId) {
        final Map<Long, Storage> storages = this.storageTypeUtil.getStorages();
        final StorageType storageType = this.storageTypeRepository.findOne(storageTypeId);
        if (storageType != null) {
            final Storage storage = storages.get(storageType.getStorageId());
            storageType.setStorage(storage);
            storageType.setStorageId(storageType.getStorageId());
            final List<StorageTypeAttributeKey> attributeKeys = this.storageAttributeRepository
                    .findByStorageTypeId(storageTypeId);
            storageType.setAttributeKeys(attributeKeys);
        }
        return storageType;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED, readOnly = false, rollbackFor = { ValidationError.class,
            ConstraintViolationException.class, DataIntegrityViolationException.class })
    public StorageType addStorageType(final StorageType storageType) throws ValidationError {
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final ValidationError v = new ValidationError();
        StorageType insertedStorageType = new StorageType();
        long storageTypeId;
        final String createdUser = storageType.getCreatedUser();
        final String time = sdf.format(timestamp);
        try {
            storageType.setUpdatedUser(createdUser);
            storageType.setCreatedTimestamp(time);
            storageType.setUpdatedTimestamp(time);
            storageType.setIsActiveYN(ActiveEnumeration.YES.getFlag());
            insertedStorageType = this.storageTypeRepository.save(storageType);
            storageTypeId = insertedStorageType.getStorageTypeId();
        }
        catch (final ConstraintViolationException e) {
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("Storage Type name is empty");
            throw v;
        }
        catch (final DataIntegrityViolationException e) {
            v.setErrorCode(HttpStatus.CONFLICT);
            v.setErrorDescription("Storage Type name is duplicated or Invalid Storage ID");
            throw v;
        }
        try {
            final List<StorageTypeAttributeKey> attributeKeys = storageType.getAttributeKeys();
            attributeKeys.forEach(attributeKey -> {
                attributeKey.setCreatedUser(createdUser);
                attributeKey.setUpdatedUser(createdUser);
                attributeKey.setCreatedTimestamp(time);
                attributeKey.setUpdatedTimestamp(time);
                attributeKey.setIsActiveYN(ActiveEnumeration.YES.getFlag());
                attributeKey.setStorageTypeId(storageTypeId);
            });
            final List<StorageTypeAttributeKey> insertedKeys = new ArrayList<StorageTypeAttributeKey>();
            this.storageAttributeRepository.save(attributeKeys).forEach(insertedKeys::add);
            insertedStorageType.setAttributeKeys(insertedKeys);
            return insertedStorageType;
        }
        catch (final ConstraintViolationException e) {
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("Storage Type Attribute name is empty");
            throw v;
        }
        catch (final DataIntegrityViolationException e) {
            v.setErrorCode(HttpStatus.CONFLICT);
            v.setErrorDescription("Storage Type Attribute name is duplicated");
            throw v;
        }
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED, readOnly = false, rollbackFor = { ValidationError.class,
            ConstraintViolationException.class, DataIntegrityViolationException.class })
    public StorageType updateStorageType(final StorageType storageType) throws ValidationError {
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final ValidationError v = new ValidationError();
        StorageType tempStorageType = this.storageTypeCache.getStorageType(storageType.getStorageTypeId());
        if (tempStorageType != null) {
            try {
                tempStorageType.setUpdatedUser(storageType.getUpdatedUser());
                tempStorageType.setUpdatedTimestamp(sdf.format(timestamp));
                this.s1.setNextChain(this.s2);
                this.s2.setNextChain(this.s3);
                this.s1.validate(storageType, tempStorageType);
                tempStorageType = this.storageTypeRepository.save(tempStorageType);
            }
            catch (final TransactionSystemException e) {
                v.setErrorCode(HttpStatus.BAD_REQUEST);
                v.setErrorDescription("Storage Type name is empty");
                throw v;
            }
            catch (final DataIntegrityViolationException e) {
                v.setErrorCode(HttpStatus.CONFLICT);
                v.setErrorDescription("Storage Type is duplicated");
                throw v;
            }
            if (storageType.getAttributeKeys() != null && storageType.getAttributeKeys().size() > 0) {
                final List<StorageTypeAttributeKey> attributeKeys = storageType.getAttributeKeys();
                attributeKeys.forEach(attributeKey -> {
                    final StorageTypeAttributeKey retrievedAttrKey = this.storageAttributeRepository
                            .findOne(attributeKey.getStorageDsAttributeKeyId());
                    retrievedAttrKey.setIsActiveYN(attributeKey.getIsActiveYN());
                    retrievedAttrKey.setUpdatedTimestamp(sdf.format(timestamp));
                    retrievedAttrKey.setStorageDsAttributeKeyName(attributeKey.getStorageDsAttributeKeyName());
                    this.storageAttributeRepository.save(retrievedAttrKey);
                });
                this.storageTypeUtil.updateAttributeKeysForObjects(attributeKeys);

            }
            return tempStorageType;

        }
        else {
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("Storage Type ID is invalid");
            throw v;
        }
    }

    @Override
    public List<StorageType> getStorageTypeByStorageCategory(final long storageId) {
        final Map<Long, Storage> storages = this.storageTypeUtil.getStorages();
        final List<StorageType> storageTypes = this.storageTypeRepository.findByStorageId(storageId);
        storageTypes.forEach(storageType -> {
            storageType.setStorage(storages.get(storageType.getStorageId()));
        });
        return storageTypes;
    }

    @Override
    public List<StorageType> getStorageTypeByStorageCategoryName(final String storageName) {

        if (storageName.equals("All")) {
            final List<StorageType> storageTypes = new ArrayList<StorageType>();
            this.storageTypeRepository.findAll().forEach(type -> storageTypes.add(type));
            return storageTypes;
        }
        else {
            final Storage storage = this.storageRepository.findByStorageName(storageName);
            final List<StorageType> storageTypes = this.storageTypeRepository.findByStorageId(storage.getStorageId());
            storageTypes.forEach(storageType -> {
                storageType.setStorage(storage);
            });
            return storageTypes;
        }

    }

    @Override
    public List<StorageTypeAttributeKey> getAllStorageAttributeKeys(final long storageTypeId) {
        final List<StorageTypeAttributeKey> attributeKeys = this.storageAttributeRepository
                .findByStorageTypeId(storageTypeId);
        return attributeKeys;
    }

    @Override
    public List<StorageTypeAttributeKey> getStorageAttributeKeys(final long storageTypeId,
            final String isStorageSystemLevel) {
        final List<StorageTypeAttributeKey> attributeKeys = this.storageAttributeRepository
                .findByStorageTypeIdAndIsStorageSystemLevelAndIsActiveYN(storageTypeId,
                        isStorageSystemLevel, "Y");
        attributeKeys.forEach(attributeKey -> {
            attributeKey.setStorageTypeAttributeValue("Defaults");
        });
        return attributeKeys;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED, readOnly = false, rollbackFor = { ValidationError.class,
            ConstraintViolationException.class, DataIntegrityViolationException.class })
    public StorageType deleteStorageType(final long storageTypeId) throws ValidationError {
        final ValidationError v = new ValidationError();
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        StorageType tempStorageType = this.storageTypeCache.getStorageType(storageTypeId);
        if (tempStorageType != null) {
            tempStorageType.setUpdatedTimestamp(sdf.format(timestamp));
            tempStorageType.setIsActiveYN(ActiveEnumeration.NO.getFlag());
            tempStorageType = this.storageTypeRepository.save(tempStorageType);
            final List<StorageTypeAttributeKey> attributeKeys = this.storageAttributeRepository
                    .findByStorageTypeIdAndIsActiveYN(storageTypeId, ActiveEnumeration.YES.getFlag());
            attributeKeys.forEach(attributeKey -> {
                attributeKey.setUpdatedTimestamp(sdf.format(timestamp));
                attributeKey.setIsActiveYN(ActiveEnumeration.NO.getFlag());
                attributeKey = this.storageAttributeRepository.save(attributeKey);
            });
            tempStorageType.setAttributeKeys(attributeKeys);
            return tempStorageType;
        }
        else {
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("Storage Type ID is invalid");
            throw v;
        }
    }

    @Override
    public StorageType enableStorageType(final long id) throws ValidationError {
        final ValidationError v = new ValidationError();
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        StorageType tempStorage = this.storageTypeCache.getStorageType(id);
        if (tempStorage != null) {
            tempStorage.setUpdatedTimestamp(sdf.format(timestamp));
            tempStorage.setIsActiveYN(ActiveEnumeration.YES.getFlag());
            tempStorage = this.storageTypeRepository.save(tempStorage);
            return tempStorage;
        }
        else {
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("Storage Type ID is invalid");
            throw v;
        }

    }

    @Override
    public StorageTypeAttributeKey updateStorageTypeAttributeKeys(final StorageTypeAttributeKey attributeKey) {
        return null;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED, readOnly = false, rollbackFor = { ValidationError.class,
            ConstraintViolationException.class, DataIntegrityViolationException.class })
    public StorageTypeAttributeKey insertStorageTypeAttributeKey(final CollectiveStorageTypeAttributeKey attributeKey)
            throws ValidationError {
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        this.storageTypeUtil.validateStorageTypeId(attributeKey.getStorageTypeId());
        this.storageTypeUtil.validateAttributes(attributeKey);
        final StorageTypeAttributeKey tempStak = this.storageAttributeRepository
                .findByStorageDsAttributeKeyNameAndStorageTypeId(attributeKey.getStorageDsAttributeKeyName(),
                        attributeKey.getStorageTypeId());
        if (tempStak != null) {
            final ValidationError v = new ValidationError();
            v.setErrorCode(HttpStatus.CONFLICT);
            v.setErrorDescription("Storage Type Attribute key and storage type ID are duplicated");
        }
        try {
            StorageTypeAttributeKey stak = new StorageTypeAttributeKey(attributeKey.getStorageDsAttributeKeyName(),
                    attributeKey.getStorageDsAttributeKeyDesc(), attributeKey.getCreatedUser(), sdf.format(timestamp),
                    attributeKey.getCreatedUser(),
                    sdf.format(timestamp), attributeKey.getStorageTypeId(),
                    ActiveEnumeration.YES.getFlag(),
                    attributeKey.getIsStorageSystemLevel());
            stak = this.storageAttributeRepository.save(stak);
            this.storageTypeUtil.insertAttributesForExistingObjects(stak);
            return stak;
        }
        catch (final DataIntegrityViolationException e) {
            final ValidationError v = new ValidationError();
            v.setErrorCode(HttpStatus.CONFLICT);
            v.setErrorDescription("Storage Type Attribute key and storage type ID are duplicated");
            throw v;
        }

    }

    @Override
    public void deleteStorageAttributeKey(final long storageTypeId, final String storageAttributeKeys) {
        // TODO

    }
}
