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
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import javax.mail.MessagingException;
import javax.validation.ConstraintViolationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import com.paypal.udc.dao.ownership.DatasetOwnershipMapRepository;
import com.paypal.udc.entity.ownership.DatasetOwnershipMap;
import com.paypal.udc.entity.storagesystem.StorageSystem;
import com.paypal.udc.exception.ValidationError;
import com.paypal.udc.service.IOwnershipDatasetService;
import com.paypal.udc.util.OwnershipUtil;
import com.paypal.udc.util.ProviderUtil;
import com.paypal.udc.util.StorageSystemUtil;
import com.paypal.udc.util.enumeration.ActiveEnumeration;


@Service
@Transactional(propagation = Propagation.SUPPORTS, readOnly = true)
public class OwnershipDatasetService implements IOwnershipDatasetService {

    final static Logger logger = LoggerFactory.getLogger(OwnershipDatasetService.class);

    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss");

    @Autowired
    private DatasetOwnershipMapRepository domr;

    @Autowired
    private ProviderUtil providerUtil;

    @Autowired
    StorageSystemUtil systemUtil;

    @Autowired
    OwnershipUtil ownershipUtil;

    @Override
    public List<DatasetOwnershipMap> getOwnersByDatasetId(final long datasetId) {

        final List<DatasetOwnershipMap> owners = this.domr.findByStorageDatasetId(datasetId);
        return owners;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED, readOnly = false, rollbackFor = { ValidationError.class,
            ConstraintViolationException.class, DataIntegrityViolationException.class, IOException.class,
            InterruptedException.class, ExecutionException.class })
    public List<DatasetOwnershipMap> addDatasetOwnershipMap(final DatasetOwnershipMap datasetOwnershipMap)
            throws ValidationError, MessagingException, IOException, InterruptedException, ExecutionException {
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final String time = sdf.format(timestamp);
        final List<DatasetOwnershipMap> ownerships = new ArrayList<DatasetOwnershipMap>();
        try {
            final long providerId = this.providerUtil.getProviderByName(datasetOwnershipMap.getProviderName());
            final StorageSystem system = this.systemUtil.getStorageSystem(datasetOwnershipMap.getStorageSystemName());
            final long systemId = system.getStorageSystemId();
            final long datasetId = this.ownershipUtil.getDatasetId(datasetOwnershipMap, system.getStorageSystemName());
            final List<DatasetOwnershipMap> existingOwnerships = datasetId > 0 ? this.domr
                    .findByStorageDatasetId(datasetId)
                    : new ArrayList<DatasetOwnershipMap>();
            if (existingOwnerships.size() > 0) {

                final List<String> metastoreEntries = existingOwnerships.stream()
                        .map(existingOwnership -> existingOwnership.getOwnerName()).collect(Collectors.toList());

                final String currentOwnerName = datasetOwnershipMap.getOwnerName();
                final String currentOtherOwnerNames = datasetOwnershipMap.getOtherOwners();
                final List<String> currentEntries = new ArrayList<String>();
                if (currentOtherOwnerNames.length() > 0) {
                    currentEntries.addAll(Arrays.asList(currentOtherOwnerNames.split(",")));
                    currentEntries.add(currentOwnerName);
                }
                else {
                    currentEntries.addAll(Arrays.asList(currentOwnerName));
                }

                final Set<String> toBeDeletedOwnerships = new HashSet<String>(metastoreEntries);
                toBeDeletedOwnerships.removeAll(currentEntries);
                final Set<String> toBeInsertedOwnerships = new HashSet<String>(currentEntries);
                toBeInsertedOwnerships.removeAll(metastoreEntries);
                final Set<String> commonOwnerships = new HashSet<String>(metastoreEntries);
                commonOwnerships.retainAll(currentEntries);
                // do a delete on ownerships

                for (final String d : toBeDeletedOwnerships) {
                    this.ownershipUtil.deleteOwnership(datasetId, d);
                }

                // // do an update on Ownerships
                final List<DatasetOwnershipMap> finalList = new ArrayList<DatasetOwnershipMap>();
                for (final String ownership : commonOwnerships) {
                    final DatasetOwnershipMap updatedOwnership = this.ownershipUtil.updateOwnership(datasetOwnershipMap,
                            datasetId,
                            systemId, ownership, providerId, time);
                    finalList.add(updatedOwnership);
                }

                // do an insert on Ownerships
                for (final String ownership : toBeInsertedOwnerships) {
                    final DatasetOwnershipMap insertOwnership = this.ownershipUtil.insertOwnership(datasetOwnershipMap,
                            datasetId, systemId, ownership, providerId, time);
                    finalList.add(insertOwnership);
                }
                // Insert entry into timelineDatasetChangeLog table for deleteOwnership
                if (toBeDeletedOwnerships.size() > 0) {
                    this.ownershipUtil.insertChangeLogForOwnership(existingOwnerships, finalList, datasetId, "D");
                }

                // Insert entry into timelineDatasetChangeLog table for InsertOwnership
                // Skip the entry to changeLog table if previous and current owners list is same
                if (!(toBeInsertedOwnerships.isEmpty() && toBeDeletedOwnerships.isEmpty())
                        && (finalList.size() > existingOwnerships.size())) {
                    this.ownershipUtil.insertChangeLogForOwnership(existingOwnerships, finalList, datasetId, "C");
                }
                return finalList;
            }
            else {
                return this.ownershipUtil.insertOwnership(ownerships, datasetOwnershipMap, datasetId, systemId,
                        providerId, time);
            }

        }
        catch (final ConstraintViolationException e) {
            final ValidationError verror = new ValidationError();
            verror.setErrorCode(HttpStatus.BAD_REQUEST);
            verror.setErrorDescription("Owner name cannot be empty");
            throw verror;
        }
        catch (final DataIntegrityViolationException e) {
            final ValidationError verror = new ValidationError();
            verror.setErrorCode(HttpStatus.BAD_REQUEST);
            verror.setErrorDescription("Container Name, Owner, Object and System already exists");
            throw verror;
        }

    }

    @Override
    public DatasetOwnershipMap updateDatasetOwnershipMap(final DatasetOwnershipMap datasetOwnershipMap)
            throws ValidationError {
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final String time = sdf.format(timestamp);
        final DatasetOwnershipMap retreivedOwnershipDatasetMap = this.domr
                .findById(datasetOwnershipMap.getDatasetOwnershipMapId()).orElse(null);
        final String updatedUser = datasetOwnershipMap.getUpdatedUser();
        if (retreivedOwnershipDatasetMap != null) {
            retreivedOwnershipDatasetMap.setOwnershipComment(datasetOwnershipMap.getOwnershipComment());
            retreivedOwnershipDatasetMap.setOwnerEmail(datasetOwnershipMap.getOwnerEmail());
            retreivedOwnershipDatasetMap.setEmailIlist(datasetOwnershipMap.getEmailIlist());
            retreivedOwnershipDatasetMap.setClaimedBy(datasetOwnershipMap.getClaimedBy());
            retreivedOwnershipDatasetMap.setUpdatedUser(updatedUser);
            retreivedOwnershipDatasetMap.setUpdatedTimestamp(time);
            return this.domr.save(retreivedOwnershipDatasetMap);
        }
        else {
            final ValidationError v = new ValidationError();
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("RetreivedOwnershipDatasetMap is invalid");
            throw v;
        }
    }

    @Override
    public DatasetOwnershipMap getOwnerBySystemContainerObjectAndOwnerName(final String storageSystemName,
            final String containerName, final String objectName, final String ownerName) throws ValidationError {
        final StorageSystem storageSystem = this.systemUtil.getStorageSystem(storageSystemName);
        if (storageSystem == null) {
            final ValidationError verror = new ValidationError();
            verror.setErrorCode(HttpStatus.BAD_REQUEST);
            verror.setErrorDescription("Invalid Storage System");
        }
        return this.domr.findByStorageSystemIdAndContainerNameAndObjectNameAndOwnerName(
                storageSystem.getStorageSystemId(), containerName, objectName, ownerName);
    }

    @Override
    public List<DatasetOwnershipMap> getOwnersBySystemContainerAndObject(final String storageSystemName,
            final String containerName, final String objectName) throws ValidationError {
        final StorageSystem storageSystem = this.systemUtil.getStorageSystem(storageSystemName);
        if (storageSystem == null) {
            final ValidationError verror = new ValidationError();
            verror.setErrorCode(HttpStatus.BAD_REQUEST);
            verror.setErrorDescription("Invalid Storage System");
        }
        return this.domr.findByStorageSystemIdAndContainerNameAndObjectName(
                storageSystem.getStorageSystemId(), containerName, objectName);
    }

    @Override
    public List<DatasetOwnershipMap> getAllOwners() {

        final List<DatasetOwnershipMap> owners = new ArrayList<DatasetOwnershipMap>();
        this.domr.findAll().forEach(owners::add);
        return owners;
    }

    @Override
    public DatasetOwnershipMap updateDatasetOwnershipNotificationStatus(final DatasetOwnershipMap datasetOwnershipMap)
            throws ValidationError {
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final String time = sdf.format(timestamp);
        final DatasetOwnershipMap retreivedOwnershipDatasetMap = this.domr
                .findById(datasetOwnershipMap.getDatasetOwnershipMapId()).orElse(null);
        final String updatedUser = datasetOwnershipMap.getUpdatedUser();
        if (retreivedOwnershipDatasetMap != null) {
            retreivedOwnershipDatasetMap.setIsNotified(datasetOwnershipMap.getIsNotified() == null
                    ? ActiveEnumeration.YES.getFlag() : datasetOwnershipMap.getIsNotified());
            retreivedOwnershipDatasetMap.setUpdatedUser(updatedUser);
            retreivedOwnershipDatasetMap.setUpdatedTimestamp(time);
            return this.domr.save(retreivedOwnershipDatasetMap);
        }
        else {
            final ValidationError v = new ValidationError();
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("RetreivedOwnershipDatasetMap is invalid");
            throw v;
        }
    }

    @Override
    public List<DatasetOwnershipMap> getAllNewOwners() {
        return this.domr.findByIsNotified(ActiveEnumeration.NO.getFlag());
    }

}
