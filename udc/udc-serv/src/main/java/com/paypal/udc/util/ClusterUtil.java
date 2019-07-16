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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import com.paypal.udc.dao.cluster.ClusterAttributeKeyRepository;
import com.paypal.udc.dao.cluster.ClusterAttributeValueRepository;
import com.paypal.udc.dao.cluster.ClusterRepository;
import com.paypal.udc.entity.cluster.Cluster;
import com.paypal.udc.entity.cluster.ClusterAttributeKey;
import com.paypal.udc.entity.cluster.ClusterAttributeKeyValue;
import com.paypal.udc.entity.cluster.ClusterAttributeValue;
import com.paypal.udc.entity.objectschema.ObjectSchemaMap;
import com.paypal.udc.entity.storagesystem.StorageSystem;
import com.paypal.udc.entity.storagesystem.StorageSystemContainer;
import com.paypal.udc.exception.ValidationError;


@Component
public class ClusterUtil {

    @Autowired
    private ClusterRepository clusterRepository;

    @Autowired
    private ClusterAttributeKeyRepository attributeKeyRepository;

    @Autowired
    private ClusterAttributeValueRepository clusterAttributeValueRepository;

    @Autowired
    private ClusterAttributeKeyRepository clusterAttributeKeyRepository;

    @Autowired
    private ClusterAttributeValueRepository attributeValueRepository;

    @Autowired
    private ObjectSchemaMapUtil schemaMapUtil;

    @Autowired
    private StorageSystemUtil storageSystemUtil;

    public Cluster validateCluster(final Long clusterId) throws ValidationError {
        final Cluster cluster = this.clusterRepository.findById(clusterId).orElse(null);
        if (cluster == null) {
            final ValidationError v = new ValidationError();
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("ClusterID is incorrect");
            throw v;
        }
        return cluster;
    }

    public void validateClusters(final List<Long> clusters) throws ValidationError {

        if (clusters == null || clusters.size() == 0) {
            final ValidationError v = new ValidationError();
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("Clusters not supplied");
            throw v;
        }
        for (final Long clusterId : clusters) {
            this.validateCluster(clusterId);
        }
    }

    public void setClusterForDefaults(final StorageSystemContainer systemContainer, final String defaultCluster) {
        Cluster cluster = this.clusterRepository.findByClusterName(defaultCluster);
        if (cluster == null) {
            cluster = this.clusterRepository.findFirstByOrderByClusterId();
            systemContainer.setClusterId(cluster.getClusterId());
        }
        else {
            systemContainer.setClusterId(cluster.getClusterId());
        }
    }

    public Map<Long, Cluster> getClusters() {
        final Map<Long, Cluster> clusters = new HashMap<Long, Cluster>();
        this.clusterRepository.findAll().forEach(cluster -> clusters.put(cluster.getClusterId(), cluster));
        return clusters;
    }

    public List<Cluster> getAllClusters() {
        final List<Cluster> clusters = new ArrayList<Cluster>();
        this.clusterRepository.findAll().forEach(cluster -> clusters.add(cluster));
        return clusters;
    }

    public Cluster getClusterByObjectId(final long objectId) throws ValidationError {
        final ObjectSchemaMap schemaMap = this.schemaMapUtil.validateObjectSchemaMapId(objectId);
        final StorageSystem storageSystem = this.storageSystemUtil
                .validateStorageSystem(schemaMap.getStorageSystemId());
        final long runningClusterId = storageSystem.getRunningClusterId();
        final Cluster cluster = this.validateCluster(runningClusterId);
        return cluster;

    }

    public List<ClusterAttributeValue> getClusterAttributeValues(final long clusterId) {

        return this.clusterAttributeValueRepository.findByClusterId(clusterId);
    }

    public List<ClusterAttributeKey> getClusterAttributeKeysByType(final String type) {
        return this.clusterAttributeKeyRepository.findByClusterAttributeKeyType(type);
    }

    public List<ClusterAttributeKeyValue> getAttributeKeyValuePairs(final long clusterId,
            final List<ClusterAttributeKey> attributeKeys) {
        final List<ClusterAttributeValue> attributeValues = this.attributeValueRepository
                .findByClusterId(clusterId);
        if (attributeValues != null && attributeValues.size() > 0) {
            final List<ClusterAttributeKeyValue> attributeKeyValues = new ArrayList<ClusterAttributeKeyValue>();
            attributeValues.stream()
                    .forEach(attributeValue -> {
                        final ClusterAttributeKey filteredAttributeKey = attributeKeys.stream()
                                .filter(attributeKey -> attributeKey
                                        .getClusterAttributeKeyId() == attributeValue
                                                .getClusterAttributeKeyId())
                                .findFirst().get();
                        final ClusterAttributeKeyValue attributeKeyValue = new ClusterAttributeKeyValue(
                                filteredAttributeKey.getClusterAttributeKeyName(),
                                attributeValue.getClusterAttributeValue(),
                                filteredAttributeKey.getClusterAttributeKeyType(),
                                filteredAttributeKey.getClusterAttributeKeyId(),
                                attributeValue.getClusterAttributeValueId());
                        attributeKeyValues.add(attributeKeyValue);
                    });
            return attributeKeyValues;
        }
        else {
            return new ArrayList<ClusterAttributeKeyValue>();
        }
    }

    public List<ClusterAttributeKey> getAllAttributeKeys() {
        final List<ClusterAttributeKey> attributeKeys = new ArrayList<ClusterAttributeKey>();
        this.attributeKeyRepository.findAll().forEach(attributeKeys::add);
        return attributeKeys;
    }
}
