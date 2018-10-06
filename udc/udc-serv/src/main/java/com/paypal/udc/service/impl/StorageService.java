package com.paypal.udc.service.impl;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import javax.validation.ConstraintViolationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.TransactionSystemException;
import com.paypal.udc.cache.StorageCache;
import com.paypal.udc.dao.StorageRepository;
import com.paypal.udc.entity.Storage;
import com.paypal.udc.exception.ValidationError;
import com.paypal.udc.service.IStorageService;
import com.paypal.udc.util.enumeration.ActiveEnumeration;
import com.paypal.udc.validator.storage.StorageDescValidator;
import com.paypal.udc.validator.storage.StorageNameValidator;


@Service
public class StorageService implements IStorageService {
    @Autowired
    private StorageRepository storageRepository;

    @Autowired
    private StorageDescValidator s2;

    @Autowired
    private StorageNameValidator s1;

    @Autowired
    private StorageCache storageCache;
    final static Logger logger = LoggerFactory.getLogger(StorageService.class);
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss");

    @Override
    public List<Storage> getAllStorages() {
        final List<Storage> storages = new ArrayList<Storage>();
        this.storageRepository.findAll().forEach(
                storage -> {
                    storages.add(storage);
                });
        return storages;
    }

    @Override
    public Storage getStorageById(final long storageId) {
        return this.storageRepository.findOne(storageId);
    }

    @Override
    public Storage addStorage(final Storage storage) throws ValidationError {
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final ValidationError v = new ValidationError();
        try {
            storage.setUpdatedUser(storage.getCreatedUser());
            storage.setCreatedTimestamp(sdf.format(timestamp));
            storage.setUpdatedTimestamp(sdf.format(timestamp));
            storage.setIsActiveYN(ActiveEnumeration.YES.getFlag());
            final Storage insertedStorage = this.storageRepository.save(storage);
            return insertedStorage;
        }
        catch (final ConstraintViolationException e) {
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("Storage name is empty");
            throw v;
        }
        catch (final DataIntegrityViolationException e) {
            v.setErrorCode(HttpStatus.CONFLICT);
            v.setErrorDescription("Storage name is duplicated");
            throw v;
        }
    }

    @Override
    public Storage updateStorage(final Storage storage) throws ValidationError {
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final ValidationError v = new ValidationError();
        Storage tempStorage = this.storageCache.getStorage(storage.getStorageId());
        if (tempStorage != null) {
            try {
                tempStorage.setUpdatedUser(storage.getUpdatedUser());
                tempStorage.setUpdatedTimestamp(sdf.format(timestamp));
                this.s1.setNextChain(this.s2);
                this.s1.validate(storage, tempStorage);
                tempStorage = this.storageRepository.save(tempStorage);
                return tempStorage;
            }
            catch (final TransactionSystemException e) {
                v.setErrorCode(HttpStatus.BAD_REQUEST);
                v.setErrorDescription("Storage name is empty");
                throw v;
            }
            catch (final DataIntegrityViolationException e) {
                v.setErrorCode(HttpStatus.CONFLICT);
                v.setErrorDescription("Storage name is duplicated");
                throw v;
            }
        }
        else {
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("Storage ID is invalid");
            throw v;
        }

    }

    @Override
    public Storage deleteStorage(final long storageId) throws ValidationError {
        final ValidationError v = new ValidationError();
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        Storage tempStorage = this.storageCache.getStorage(storageId);
        if (tempStorage != null) {
            tempStorage.setUpdatedTimestamp(sdf.format(timestamp));
            tempStorage.setIsActiveYN(ActiveEnumeration.NO.getFlag());
            tempStorage = this.storageRepository.save(tempStorage);
            return tempStorage;
        }
        else {
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("Storage ID is invalid");
            throw v;
        }

    }

    @Override
    public Storage getStorageByName(final String storageName) {
        return this.storageRepository.findByStorageName(storageName);
    }

    @Override
    public Storage enableStorage(final long id) throws ValidationError {
        final ValidationError v = new ValidationError();
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        Storage tempStorage = this.storageCache.getStorage(id);
        if (tempStorage != null) {
            tempStorage.setUpdatedTimestamp(sdf.format(timestamp));
            tempStorage.setIsActiveYN(ActiveEnumeration.YES.getFlag());
            tempStorage = this.storageRepository.save(tempStorage);
            return tempStorage;
        }
        else {
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("Storage ID is invalid");
            throw v;
        }

    }

}
