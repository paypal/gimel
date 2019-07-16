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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import com.google.gson.Gson;
import com.paypal.udc.dao.cluster.ClusterRepository;
import com.paypal.udc.dao.storagesystem.ElasticSearchStorageSystemRepository;
import com.paypal.udc.dao.storagesystem.StorageSystemAttributeValueRepository;
import com.paypal.udc.dao.storagesystem.StorageSystemContainerRepository;
import com.paypal.udc.dao.storagesystem.StorageSystemRepository;
import com.paypal.udc.dao.storagetype.StorageTypeRepository;
import com.paypal.udc.entity.cluster.Cluster;
import com.paypal.udc.entity.storagesystem.ElasticSearchStorageSystem;
import com.paypal.udc.entity.storagesystem.StorageSystem;
import com.paypal.udc.entity.storagesystem.StorageSystemAttributeValue;
import com.paypal.udc.entity.storagetype.StorageType;
import com.paypal.udc.entity.storagetype.StorageTypeAttributeKey;
import com.paypal.udc.exception.ValidationError;
import com.paypal.udc.service.impl.StorageSystemService;
import com.paypal.udc.util.enumeration.ActiveEnumeration;
import com.paypal.udc.util.enumeration.DiscoveryStatusEnumeration;
import com.paypal.udc.util.enumeration.RunFrequencyEnumeration;


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
    @Autowired
    private ElasticSearchStorageSystemRepository esStorageSystemRepository;
    @Autowired
    private TransportClient client;
    @Autowired
    private StorageSystemAttributeValueRepository systemAttributeValueRepository;
    @Autowired
    private StorageSystemContainerRepository storageSystemContainerRepository;

    public Map<Long, StorageSystem> getStorageSystems() {
        final Map<Long, StorageSystem> storageSystemMap = new HashMap<Long, StorageSystem>();
        this.storageSystemRepository.findAll()
                .forEach(storageSystem -> {
                    storageSystemMap.put(storageSystem.getStorageSystemId(), storageSystem);
                });
        return storageSystemMap;
    }

    public Map<String, StorageSystem> getStorageSystemsByName() {
        final Map<String, StorageSystem> storageSystemMap = new HashMap<String, StorageSystem>();
        this.storageSystemRepository.findAll()
                .forEach(storageSystem -> {
                    storageSystemMap.put(storageSystem.getStorageSystemName(), storageSystem);
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

    public StorageSystem validateStorageSystem(final long storageSystemId) throws ValidationError {
        final StorageSystem storageSystem = this.storageSystemRepository.findById(storageSystemId)
                .orElse(null);
        if (storageSystem == null) {
            final ValidationError verror = new ValidationError();
            verror.setErrorCode(HttpStatus.BAD_REQUEST);
            verror.setErrorDescription("Invalid Storage System");
            throw verror;
        }
        return storageSystem;
    }

    public StorageType getStorageType(final long storageSystemId) throws ValidationError {
        final StorageSystem storageSystem = this.storageSystemRepository.findById(storageSystemId)
                .orElse(null);
        if (storageSystem != null) {
            final StorageType storageType = this.storageTypeUtil.getStorageType(storageSystem.getStorageTypeId());
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

    public void validateRunFrequency(final String runFrequency) throws ValidationError {
        final List<String> frequencies = Arrays.asList(RunFrequencyEnumeration.values()).stream()
                .map(frequency -> frequency.getFlag()).collect(Collectors.toList());
        if (!frequencies.contains(runFrequency)) {
            final ValidationError verror = new ValidationError();
            verror.setErrorCode(HttpStatus.BAD_REQUEST);
            verror.setErrorDescription("Invalid Run Frequency Supplied");
            throw verror;
        }
    }

    public void validateDiscoveryStatus(final String discoveryStatus) throws ValidationError {
        final List<String> statusList = Arrays.asList(DiscoveryStatusEnumeration.values()).stream()
                .map(status -> status.getFlag()).collect(Collectors.toList());

        if (!statusList.contains(discoveryStatus)) {
            final ValidationError verror = new ValidationError();
            verror.setErrorCode(HttpStatus.BAD_REQUEST);
            verror.setErrorDescription("Invalid Discovery status Supplied");
            throw verror;
        }

    }

    public void validateDates(final String startTime, final String endTime, final SimpleDateFormat sdf)
            throws ParseException, ValidationError {
        final Date d1 = sdf.parse(startTime);
        final Date d2 = sdf.parse(endTime);
        final long elapsed = d2.getTime() - d1.getTime();
        if (elapsed < 0) {
            final ValidationError verror = new ValidationError();
            verror.setErrorCode(HttpStatus.BAD_REQUEST);
            verror.setErrorDescription("End Date is earlier to Start Date");
            throw verror;
        }
    }

    public void upsertStorageSystem(final String esSystemIndex, final String esType, final StorageSystem storageSystem,
            final ElasticsearchTemplate esTemplate) throws IOException, InterruptedException, ExecutionException {
        final List<ElasticSearchStorageSystem> esStorageSystem = this.esStorageSystemRepository
                .findByStorageSystemId((storageSystem.getStorageSystemId()));
        if (esStorageSystem == null || esStorageSystem.isEmpty()) {
            this.save(esSystemIndex, esType, storageSystem);
            esTemplate.refresh(esSystemIndex);
        }
        else {
            this.update(esSystemIndex, esType, esStorageSystem.get(0), storageSystem);
            esTemplate.refresh(esSystemIndex);
        }
    }

    public void save(final String indexName, final String typeName, final StorageSystem storageSystem) {
        final Gson gson = new Gson();

        final ElasticSearchStorageSystem esStorageSystem = new ElasticSearchStorageSystem(
                storageSystem.getStorageSystemId(), storageSystem.getStorageSystemName(),
                storageSystem.getStorageSystemDescription(), storageSystem.getIsActiveYN(),
                storageSystem.getAdminUserId(), storageSystem.getZoneId(),
                storageSystem.getIsReadCompatible(),
                storageSystem.getRunningClusterId(), storageSystem.getCreatedUser(),
                storageSystem.getCreatedTimestamp(), storageSystem.getUpdatedUser(),
                storageSystem.getUpdatedTimestamp(), storageSystem.getStorageTypeId(),
                storageSystem.getSystemAttributeValues(), storageSystem.getEntityId());

        final IndexResponse response = this.client.prepareIndex(indexName, typeName, esStorageSystem.get_id())
                .setSource(gson.toJson(esStorageSystem), XContentType.JSON)
                .get();
    }

    public void update(final String indexName, final String typeName, final ElasticSearchStorageSystem esStorageSystem,
            final StorageSystem system) throws IOException, InterruptedException, ExecutionException {

        final Gson gson = new Gson();
        final List<StorageSystemAttributeValue> systemAttributeValues = this.systemAttributeValueRepository
                .findByStorageSystemIdAndIsActiveYN(system.getStorageSystemId(),
                        system.getIsActiveYN());
        this.storageSystemContainerRepository.findByStorageSystemId(system.getStorageSystemId());
        system.setSystemAttributeValues(systemAttributeValues);
        final UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index(indexName).type(typeName)
                .id(esStorageSystem.get_id())
                .doc(gson.toJson(system), XContentType.JSON);
        final UpdateResponse updateResponse = this.client.update(updateRequest).get();

    }

}
