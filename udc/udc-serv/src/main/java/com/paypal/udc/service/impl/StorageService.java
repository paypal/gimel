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
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import javax.validation.ConstraintViolationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import com.paypal.udc.dao.storagecategory.StorageRepository;
import com.paypal.udc.entity.storagecategory.Storage;
import com.paypal.udc.exception.ValidationError;
import com.paypal.udc.service.IStorageService;
import com.paypal.udc.util.StorageUtil;
import com.paypal.udc.util.enumeration.ActiveEnumeration;
import com.paypal.udc.validator.storage.StorageDescValidator;
import com.paypal.udc.validator.storage.StorageNameValidator;


@Service
@Transactional(propagation = Propagation.SUPPORTS, readOnly = true)
public class StorageService implements IStorageService {
    @Autowired
    private StorageRepository storageRepository;

    @Autowired
    private StorageDescValidator s2;

    @Autowired
    private StorageNameValidator s1;

    @Autowired
    private StorageUtil storageUtil;

    @Value("${elasticsearch.category.index.name}")
    private String esStorageCategoryIndex;

    @Value("${elasticsearch.type.name}")
    private String esType;

    @Value("${udc.es.write.enabled}")
    private String isEsWriteEnabled;

    @Autowired
    private ElasticsearchTemplate esTemplate;

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
    public Storage getStorageById(final long storageId) throws ValidationError {
        final Storage storage = this.storageUtil.validateStorageId(storageId);
        return storage;

    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED, readOnly = false, rollbackFor = { ValidationError.class,
            ConstraintViolationException.class, DataIntegrityViolationException.class,
            TransactionSystemException.class, IOException.class, InterruptedException.class, ExecutionException.class })
    public Storage addStorage(final Storage storage)
            throws ValidationError, IOException, InterruptedException, ExecutionException {
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final ValidationError v = new ValidationError();
        final Storage insertedStorage;
        try {
            storage.setUpdatedUser(storage.getCreatedUser());
            storage.setCreatedTimestamp(sdf.format(timestamp));
            storage.setUpdatedTimestamp(sdf.format(timestamp));
            storage.setIsActiveYN(ActiveEnumeration.YES.getFlag());
            insertedStorage = this.storageRepository.save(storage);

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

        if (this.isEsWriteEnabled.equals("true")) {
            this.storageUtil.upsertStorage(this.esStorageCategoryIndex, this.esType, storage, this.esTemplate);
        }

        return insertedStorage;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED, readOnly = false, rollbackFor = { ValidationError.class,
            ConstraintViolationException.class, DataIntegrityViolationException.class,
            TransactionSystemException.class, IOException.class, InterruptedException.class, ExecutionException.class })
    public Storage updateStorage(final Storage storage)
            throws ValidationError, IOException, InterruptedException, ExecutionException {
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final ValidationError v = new ValidationError();
        Storage tempStorage = this.storageUtil.validateStorageId(storage.getStorageId());
        try {
            tempStorage.setUpdatedUser(storage.getUpdatedUser());
            tempStorage.setUpdatedTimestamp(sdf.format(timestamp));
            this.s1.setNextChain(this.s2);
            this.s1.validate(storage, tempStorage);
            tempStorage = this.storageRepository.save(tempStorage);

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

        if (this.isEsWriteEnabled.equals("true")) {
            this.storageUtil.upsertStorage(this.esStorageCategoryIndex, this.esType, tempStorage, this.esTemplate);
        }
        return tempStorage;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED, readOnly = false, rollbackFor = { ValidationError.class,
            ConstraintViolationException.class, DataIntegrityViolationException.class,
            TransactionSystemException.class, IOException.class, InterruptedException.class, ExecutionException.class })
    public Storage deleteStorage(final long storageId)
            throws ValidationError, IOException, InterruptedException, ExecutionException {
        final ValidationError v = new ValidationError();
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        Storage tempStorage = this.storageUtil.validateStorageId(storageId);

        tempStorage.setUpdatedTimestamp(sdf.format(timestamp));
        tempStorage.setIsActiveYN(ActiveEnumeration.NO.getFlag());
        tempStorage = this.storageRepository.save(tempStorage);
        if (this.isEsWriteEnabled.equals("true")) {
            this.storageUtil.upsertStorage(this.esStorageCategoryIndex, this.esType, tempStorage, this.esTemplate);
        }
        return tempStorage;
    }

    @Override
    public Storage getStorageByName(final String storageName) {
        return this.storageRepository.findByStorageName(storageName);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED, readOnly = false, rollbackFor = { ValidationError.class,
            ConstraintViolationException.class, DataIntegrityViolationException.class,
            TransactionSystemException.class, IOException.class, InterruptedException.class, ExecutionException.class })
    public Storage enableStorage(final long id)
            throws ValidationError, IOException, InterruptedException, ExecutionException {
        final ValidationError v = new ValidationError();
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        Storage tempStorage = this.storageUtil.validateStorageId(id);
        tempStorage.setUpdatedTimestamp(sdf.format(timestamp));
        tempStorage.setIsActiveYN(ActiveEnumeration.YES.getFlag());
        tempStorage = this.storageRepository.save(tempStorage);
        if (this.isEsWriteEnabled.equals("true")) {
            this.storageUtil.upsertStorage(this.esStorageCategoryIndex, this.esType, tempStorage, this.esTemplate);
        }
        return tempStorage;
    }

}
