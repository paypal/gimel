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

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
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
import com.paypal.udc.dao.storagesystem.StorageSystemContainerRepository;
import com.paypal.udc.dao.storagetype.StorageTypeAttributeKeyRepository;
import com.paypal.udc.entity.storagesystem.CollectiveStorageSystemContainerObject;
import com.paypal.udc.entity.storagesystem.StorageSystem;
import com.paypal.udc.entity.storagesystem.StorageSystemAttributeValue;
import com.paypal.udc.entity.storagesystem.StorageSystemContainer;
import com.paypal.udc.entity.storagetype.StorageTypeAttributeKey;
import com.paypal.udc.entity.user.User;
import com.paypal.udc.exception.ValidationError;
import com.paypal.udc.service.IStorageSystemContainerService;
import com.paypal.udc.util.StorageSystemUtil;
import com.paypal.udc.util.UserUtil;
import com.paypal.udc.util.enumeration.ActiveEnumeration;


@Service
public class StorageSystemContainerService implements IStorageSystemContainerService {

    @Autowired
    private StorageSystemContainerRepository storageSystemContainerRepository;

    @Autowired
    private StorageSystemUtil storageSystemUtil;

    @Autowired
    private StorageTypeAttributeKeyRepository stakr;

    @Autowired
    private UserUtil userUtil;

    final static Logger logger = LoggerFactory.getLogger(StorageSystemContainerService.class);
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss");

    @Override
    public StorageSystemContainer getStorageSystemContainerById(final long storageSystemContainerId)
            throws ValidationError {
        final ValidationError v = new ValidationError();
        final StorageSystemContainer storageSystemContainer = this.storageSystemContainerRepository
                .findById(storageSystemContainerId)
                .orElse(null);
        if (storageSystemContainer != null) {
            return storageSystemContainer;
        }
        else {
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("storageSystemContainer ID is invalid");
            throw v;
        }

    }

    @Override
    public List<CollectiveStorageSystemContainerObject> getAllStorageSystemContainers(final long clusterId)
            throws ValidationError {

        final List<CollectiveStorageSystemContainerObject> storageSystemContainersWithAttributes = new ArrayList<CollectiveStorageSystemContainerObject>();
        final Map<Long, StorageSystem> storageSystemMap = this.storageSystemUtil.getStorageSystems();

        final Map<Long, CollectiveStorageSystemContainerObject> storageSystemPropertyMap = new HashMap<Long, CollectiveStorageSystemContainerObject>();

        final List<StorageSystemContainer> storageSystemContainers = new ArrayList<StorageSystemContainer>();

        final List<StorageSystemContainer> storageSystemContainersList = this.storageSystemContainerRepository
                .findByClusterId(clusterId);

        for (final StorageSystemContainer storageSystemContainer : storageSystemContainersList) {
            if (storageSystemContainer.getIsActiveYN().equals(ActiveEnumeration.YES.getFlag())) {
                storageSystemContainers.add(storageSystemContainer);
            }
        }
        for (final StorageSystemContainer storageSystemContainer : storageSystemContainers) {
            final CollectiveStorageSystemContainerObject object = new CollectiveStorageSystemContainerObject();
            final long storageSystemId = storageSystemContainer.getStorageSystemId();
            final StorageSystem storageSystem = storageSystemMap.get(storageSystemId);
            final User user = this.userUtil.validateUser(storageSystem.getAdminUserId());
            final String userName = user.getUserName();
            object.setUserName(userName);
            object.setStorageSystemId(storageSystemId);
            object.setContainerName(storageSystemContainer.getContainerName());
            object.setStorageSystemName(storageSystem.getStorageSystemName());

            if (storageSystemPropertyMap.get(storageSystemId) != null) {
                final CollectiveStorageSystemContainerObject tempObject = storageSystemPropertyMap.get(storageSystemId);
                object.setSystemAttributes(tempObject.getSystemAttributes());
                object.setTypeAttributes(tempObject.getTypeAttributes());
            }
            else {
                final List<StorageTypeAttributeKey> typeAttributes = this.stakr
                        .findByStorageTypeIdAndIsStorageSystemLevelAndIsActiveYN(
                                storageSystem.getStorageTypeId(), ActiveEnumeration.NO.getFlag(),
                                ActiveEnumeration.YES.getFlag());
                final List<StorageSystemAttributeValue> systemAttributes = this.storageSystemUtil
                        .getAttributes(storageSystemId);
                object.setTypeAttributes(typeAttributes);
                object.setSystemAttributes(systemAttributes);
                storageSystemPropertyMap.put(storageSystemId, object);
            }
            storageSystemContainersWithAttributes.add(object);
        }
        return storageSystemContainersWithAttributes;
    }

    @Override
    public List<StorageSystemContainer> getStorageSystemContainersByStorageSystemId(final long storageId) {

        final List<StorageSystemContainer> storageSystemContainers = new ArrayList<StorageSystemContainer>();
        this.storageSystemContainerRepository.findByStorageSystemId(storageId).forEach(
                storageSystemContainer -> {
                    if (storageSystemContainer.getIsActiveYN().equals(ActiveEnumeration.YES.getFlag())) {
                        storageSystemContainers.add(storageSystemContainer);
                    }
                });
        return storageSystemContainers;
    }

    @Override
    public StorageSystemContainer deleteStorageSystemContainer(final long storageSystemContainerId)
            throws ValidationError {

        final ValidationError v = new ValidationError();
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        StorageSystemContainer tempCluster = this.storageSystemContainerRepository.findById(storageSystemContainerId)
                .orElse(null);
        if (tempCluster != null) {
            tempCluster.setUpdatedTimestamp(sdf.format(timestamp));
            tempCluster.setIsActiveYN(ActiveEnumeration.NO.getFlag());
            tempCluster = this.storageSystemContainerRepository.save(tempCluster);
            return tempCluster;
        }
        else {
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("storageSystemContainer ID is invalid");
            throw v;
        }
    }

    @Override
    public StorageSystemContainer addStorageSystemContainer(final StorageSystemContainer storageSystemContainer)
            throws ValidationError {

        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final ValidationError v = new ValidationError();
        try {
            storageSystemContainer.setUpdatedUser(storageSystemContainer.getCreatedUser());
            storageSystemContainer.setCreatedTimestamp(sdf.format(timestamp));
            storageSystemContainer.setUpdatedTimestamp(sdf.format(timestamp));
            storageSystemContainer.setIsActiveYN(ActiveEnumeration.YES.getFlag());
            final StorageSystemContainer insertedCluster = this.storageSystemContainerRepository
                    .save(storageSystemContainer);
            return insertedCluster;
        }
        catch (final ConstraintViolationException e) {
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("Storage System Container is empty");
            throw v;
        }
        catch (final DataIntegrityViolationException e) {
            v.setErrorCode(HttpStatus.CONFLICT);
            v.setErrorDescription("Storage System Container is duplicated");
            throw v;
        }
    }

    @Override
    public StorageSystemContainer updateStorageSystemContainer(final StorageSystemContainer storageSystemContainer)
            throws ValidationError {
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final ValidationError v = new ValidationError();
        StorageSystemContainer tempStorageSystemContainer = this.storageSystemContainerRepository
                .findById(storageSystemContainer.getStorageSystemContainerId()).orElse(null);
        if (tempStorageSystemContainer != null) {
            try {
                tempStorageSystemContainer.setStorageSystemId(storageSystemContainer.getStorageSystemId());
                tempStorageSystemContainer.setIsActiveYN(storageSystemContainer.getIsActiveYN());
                tempStorageSystemContainer.setContainerName(storageSystemContainer.getContainerName());
                tempStorageSystemContainer.setUpdatedUser(storageSystemContainer.getCreatedUser());
                tempStorageSystemContainer.setUpdatedTimestamp(sdf.format(timestamp));
                // this.s1.setNextChain(this.s2);
                // this.s1.validate(storageSystemContainer, tempStorageSystemContainer);
                tempStorageSystemContainer = this.storageSystemContainerRepository.save(tempStorageSystemContainer);
                return tempStorageSystemContainer;
            }
            catch (final TransactionSystemException e) {
                v.setErrorCode(HttpStatus.BAD_REQUEST);
                v.setErrorDescription("Storage System Container is empty --> " + tempStorageSystemContainer.toString());
                throw v;
            }
            catch (final DataIntegrityViolationException e) {
                v.setErrorCode(HttpStatus.CONFLICT);
                v.setErrorDescription(
                        "Storage System Container is duplicated -->" + tempStorageSystemContainer.toString());
                throw v;
            }
        }
        else {
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription(
                    "Storage System Container ID is invalid --> " + tempStorageSystemContainer.toString());
            throw v;
        }

    }
}
