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

package com.paypal.udc.util;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.stereotype.Component;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.paypal.udc.dao.dataset.DatasetChangeLogRepository;
import com.paypal.udc.dao.dataset.DatasetRepository;
import com.paypal.udc.dao.ownership.DatasetOwnershipMapRepository;
import com.paypal.udc.entity.dataset.Dataset;
import com.paypal.udc.entity.dataset.DatasetChangeLog;
import com.paypal.udc.entity.ownership.DatasetOwnershipMap;
import com.paypal.udc.exception.ValidationError;
import com.paypal.udc.service.impl.OwnershipDatasetService;
import com.paypal.udc.util.enumeration.ActiveEnumeration;
import com.paypal.udc.util.enumeration.TimelineEnumeration;


@Component
public class OwnershipUtil {

    final static Logger logger = LoggerFactory.getLogger(OwnershipDatasetService.class);
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss");

    @Autowired
    private DatasetChangeLogRepository changeLogRepository;

    @Autowired
    private DatasetOwnershipMapRepository domr;

    @Autowired
    DatasetRepository datasetRepository;

    @Value("${elasticsearch.type.name}")
    private String esType;

    @Value("${udc.es.write.enabled}")
    private String isEsWriteEnabled;

    @Autowired
    private ElasticsearchTemplate esTemplate;

    @Autowired
    private DatasetUtil datasetUtil;

    @Value("${elasticsearch.dataset.name}")
    private String esDatasetIndex;

    public void deleteOwnership(final long datasetId, final String ownerName) {
        final DatasetOwnershipMap datasetOwner = this.domr.findByStorageDatasetIdAndOwnerName(datasetId, ownerName);
        this.domr.delete(datasetOwner);
    }

    public DatasetOwnershipMap insertOwnership(final DatasetOwnershipMap datasetOwnershipMap,
            final long datasetId, final long systemId, final String ownerName, final long providerId,
            final String time) throws ValidationError, IOException, InterruptedException, ExecutionException {
        datasetOwnershipMap.setStorageDatasetId(datasetId);
        datasetOwnershipMap.setStorageSystemId(systemId);
        datasetOwnershipMap.setProviderId(providerId);
        datasetOwnershipMap.setUpdatedTimestamp(time);
        datasetOwnershipMap.setOwnerName(ownerName);
        datasetOwnershipMap.setUpdatedUser(datasetOwnershipMap.getCreatedUser());
        datasetOwnershipMap.setClaimedBy(datasetOwnershipMap.getCreatedUser());
        datasetOwnershipMap.setCreatedTimestamp(time);
        datasetOwnershipMap.setIsNotified(ActiveEnumeration.NO.getFlag());
        this.domr.save(datasetOwnershipMap);
        if (this.isEsWriteEnabled.equals("true")) {
            final Dataset tempDataset = this.datasetRepository.findById(datasetOwnershipMap.getStorageDatasetId())
                    .orElse(null);
            if (tempDataset != null) {
                this.datasetUtil.upsertDataset(this.esDatasetIndex, this.esType, tempDataset, this.esTemplate);
            }
        }
        return datasetOwnershipMap;
    }

    public DatasetOwnershipMap updateOwnership(final DatasetOwnershipMap datasetOwnershipMap, final long datasetId,
            final long systemId, final String ownerName, final long providerId, final String time)
            throws ValidationError, IOException, InterruptedException, ExecutionException {
        final DatasetOwnershipMap datasetOwner = this.domr.findByStorageDatasetIdAndOwnerName(datasetId, ownerName);

        final String prevEmailIlist = datasetOwner.getEmailIlist();
        final String currEmailIlist = datasetOwnershipMap.getEmailIlist();
        final String prevComment = datasetOwner.getOwnershipComment();
        final String currComment = datasetOwnershipMap.getOwnershipComment();
        final String prevUser = datasetOwner.getUpdatedUser();
        datasetOwner.setStorageDatasetId(datasetId);
        datasetOwner.setStorageSystemId(systemId);
        datasetOwner.setProviderId(providerId);
        datasetOwner.setUpdatedTimestamp(time);
        datasetOwner.setUpdatedUser(datasetOwnershipMap.getCreatedUser());
        datasetOwner.setCreatedTimestamp(time);
        datasetOwner.setEmailIlist(datasetOwnershipMap.getEmailIlist());
        datasetOwner.setOwnershipComment(datasetOwnershipMap.getOwnershipComment());
        datasetOwner.setIsNotified(ActiveEnumeration.NO.getFlag());
        // Insert entry into timelineDatasetChangeLog table for ownership description changes
        final Gson gson = new Gson();
        final Map<String, String> currentKeyValue = new HashMap<String, String>();
        currentKeyValue.put("value", currComment);
        currentKeyValue.put("ownerName", datasetOwner.getOwnerName());
        currentKeyValue.put("username", datasetOwner.getUpdatedUser());
        final String curr = gson.toJson(currentKeyValue);
        final Map<String, String> prevKeyValue = new HashMap<String, String>();
        prevKeyValue.put("value", prevComment);
        currentKeyValue.put("ownerName", datasetOwner.getOwnerName());
        prevKeyValue.put("username", prevUser);
        final String prev = gson.toJson(prevKeyValue);

        if (!(prevComment.equalsIgnoreCase(currComment))) {
            final DatasetChangeLog dcl = new DatasetChangeLog(datasetId, "M",
                    TimelineEnumeration.OWNERSHIP.getFlag(), prev, curr,
                    time);
            this.changeLogRepository.save(dcl);
        }
        // Insert entry into timelineDatasetChangeLog table for ownership emailList changes
        final Map<String, String> currentEmailIListKeyValue = new HashMap<String, String>();
        currentEmailIListKeyValue.put("value", currEmailIlist);
        currentEmailIListKeyValue.put("ownerName", datasetOwner.getOwnerName());
        currentEmailIListKeyValue.put("username", datasetOwner.getUpdatedUser());
        final String currEmailIListKeyValue = gson.toJson(currentEmailIListKeyValue);
        final Map<String, String> prevEmailIListKeyValue = new HashMap<String, String>();
        prevEmailIListKeyValue.put("value", prevEmailIlist);
        prevEmailIListKeyValue.put("ownerName", datasetOwner.getOwnerName());
        prevEmailIListKeyValue.put("username", prevUser);
        final String previousEmailIListKeyValue = gson.toJson(prevEmailIListKeyValue);

        if (!(prevEmailIlist.equalsIgnoreCase(currEmailIlist))) {
            final DatasetChangeLog dcl = new DatasetChangeLog(datasetId, "M",
                    TimelineEnumeration.OWNERSHIP.getFlag(), previousEmailIListKeyValue, currEmailIListKeyValue,
                    time);
            this.changeLogRepository.save(dcl);
        }

        if (this.isEsWriteEnabled.equals("true")) {
            final Dataset tempDataset = this.datasetRepository.findById(datasetOwnershipMap.getStorageDatasetId())
                    .orElse(null);
            if (tempDataset != null) {
                this.datasetUtil.upsertDataset(this.esDatasetIndex, this.esType, tempDataset, this.esTemplate);
            }
        }
        return datasetOwner;
    }

    public List<DatasetOwnershipMap> insertOwnership(final List<DatasetOwnershipMap> ownerships,
            final DatasetOwnershipMap datasetOwnershipMap, final long datasetId,
            final long systemId, final long providerId, final String time)
            throws ValidationError, IOException, InterruptedException, ExecutionException {

        datasetOwnershipMap.setStorageDatasetId(datasetId);
        datasetOwnershipMap.setStorageSystemId(systemId);
        datasetOwnershipMap.setProviderId(providerId);
        datasetOwnershipMap.setUpdatedTimestamp(time);
        datasetOwnershipMap.setUpdatedUser(datasetOwnershipMap.getCreatedUser());
        datasetOwnershipMap.setClaimedBy(datasetOwnershipMap.getOwnerName());
        datasetOwnershipMap.setCreatedTimestamp(time);
        datasetOwnershipMap.setIsNotified(ActiveEnumeration.NO.getFlag());
        ownerships.add(datasetOwnershipMap);

        final String miscOwners = datasetOwnershipMap.getOtherOwners();
        if (miscOwners.length() > 0) {
            final List<String> miscOwnersArray = Arrays.asList(miscOwners.split(","));
            miscOwnersArray.forEach(owner -> {
                final DatasetOwnershipMap datasetOwner = new DatasetOwnershipMap(datasetId, providerId,
                        systemId, owner, datasetOwnershipMap.getContainerName(),
                        datasetOwnershipMap.getObjectName(), datasetOwnershipMap.getOwnershipComment(),
                        owner, datasetOwnershipMap.getEmailIlist(),
                        datasetOwnershipMap.getCreatedUser(), datasetOwnershipMap.getCreatedTimestamp(),
                        datasetOwnershipMap.getUpdatedUser(), datasetOwnershipMap.getUpdatedTimestamp(),
                        datasetOwnershipMap.getOwnerName(), ActiveEnumeration.NO.getFlag());
                ownerships.add(datasetOwner);
            });
        }
        final List<DatasetOwnershipMap> prevOwnerships = new ArrayList<DatasetOwnershipMap>();
        this.insertChangeLogForOwnership(prevOwnerships, ownerships, datasetId, "C");
        this.domr.saveAll(ownerships);
        for (final DatasetOwnershipMap i : ownerships) {
            if (this.isEsWriteEnabled.equals("true")) {
                final Dataset tempDataset = this.datasetRepository.findById(i.getStorageDatasetId())
                        .orElse(null);
                if (tempDataset != null) {
                    this.datasetUtil.upsertDataset(this.esDatasetIndex, this.esType, tempDataset, this.esTemplate);
                }
            }
        }
        return Lists.newArrayList(ownerships);
    }

    public long getDatasetId(final DatasetOwnershipMap datasetOwnershipMap, final String systemName) {
        long datasetId = datasetOwnershipMap.getStorageDatasetId();

        if (datasetId == 0) {
            final String datasetName = systemName + "." + datasetOwnershipMap.getContainerName()
                    + "." + datasetOwnershipMap.getObjectName();
            final Dataset dataset = this.datasetRepository.findByStorageDataSetName(datasetName);
            datasetId = datasetOwnershipMap.getStorageDatasetId() == 0
                    ? (dataset == null ? 0 : dataset.getStorageDataSetId())
                    : datasetOwnershipMap.getStorageDatasetId();
        }

        return datasetId;
    }

    public List<DatasetOwnershipMap> getOwnersByDatasetId(final long storageDatasetId) {
        final List<DatasetOwnershipMap> datasetOwnerMap = this.domr.findByStorageDatasetId(storageDatasetId);
        return datasetOwnerMap;
    }

    public void insertChangeLogForOwnership(final List<DatasetOwnershipMap> prevOwnerships,
            final List<DatasetOwnershipMap> currOwnerships, final long datasetId, final String changeType) {
        final Gson gson = new Gson();
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final String time = sdf.format(timestamp);
        final ArrayList<String> currOwnershipList = new ArrayList<String>();
        final ArrayList<String> prevOwnershipList = new ArrayList<String>();
        final Map<String, String> prevKeyValue = new HashMap<String, String>();
        final Map<String, String> currentKeyValue = new HashMap<String, String>();
        // Get list of previous Owners
        if (prevOwnerships.size() > 0) {
            prevOwnerships.forEach((owner) -> {
                prevKeyValue.put("value", owner.getOwnerName());
                prevKeyValue.put("username", owner.getCreatedUser());
                final String prev = gson.toJson(prevKeyValue);
                prevOwnershipList.add(prev);
            });

        }
        // Get list of current Owners
        currOwnerships.forEach((owner) -> {
            currentKeyValue.put("value", owner.getOwnerName());
            currentKeyValue.put("username", owner.getCreatedUser());
            final String curr = gson.toJson(currentKeyValue);
            currOwnershipList.add(curr);
        });

        final DatasetChangeLog dcl = new DatasetChangeLog(datasetId, changeType,
                TimelineEnumeration.OWNERSHIP.getFlag(), prevOwnershipList.toString(), currOwnershipList.toString(),
                time);
        this.changeLogRepository.save(dcl);

    }

}
