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
import java.util.List;
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
import com.paypal.udc.dao.cluster.ClusterAttributeValueRepository;
import com.paypal.udc.dao.cluster.ClusterRepository;
import com.paypal.udc.entity.cluster.Cluster;
import com.paypal.udc.entity.cluster.ClusterAttributeKey;
import com.paypal.udc.entity.cluster.ClusterAttributeKeyValue;
import com.paypal.udc.entity.cluster.ClusterAttributeValue;
import com.paypal.udc.exception.ValidationError;
import com.paypal.udc.service.IClusterService;
import com.paypal.udc.util.ClusterUtil;
import com.paypal.udc.util.enumeration.ActiveEnumeration;
import com.paypal.udc.validator.cluster.ClusterDescValidator;
import com.paypal.udc.validator.cluster.ClusterNameValidator;


@Service
@Transactional(propagation = Propagation.SUPPORTS, readOnly = true)
public class ClusterService implements IClusterService {
    @Autowired
    private ClusterRepository clusterRepository;

    @Autowired
    private ClusterDescValidator s2;

    @Autowired
    private ClusterNameValidator s1;

    @Autowired
    private ClusterUtil clusterUtil;

    @Autowired
    private ClusterAttributeValueRepository cavr;

    final static Logger logger = LoggerFactory.getLogger(ClusterService.class);
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss");

    @Override
    public List<Cluster> getAllClusters() {
        final List<Cluster> clusters = new ArrayList<Cluster>();
        final List<ClusterAttributeKey> attributeKeys = this.clusterUtil.getAllAttributeKeys();
        this.clusterRepository.findAll().forEach(
                cluster -> {
                    final List<ClusterAttributeKeyValue> attributeKeyValues = this.clusterUtil
                            .getAttributeKeyValuePairs(cluster.getClusterId(), attributeKeys);
                    cluster.setAttributeKeyValues(attributeKeyValues);
                    clusters.add(cluster);
                });
        return clusters;
    }

    @Override
    public Cluster getClusterById(final long clusterId) throws ValidationError {
        final List<ClusterAttributeKey> attributeKeys = this.clusterUtil.getAllAttributeKeys();
        final Cluster cluster = this.clusterUtil.validateCluster(clusterId);
        final List<ClusterAttributeKeyValue> attributeKeyValues = this.clusterUtil
                .getAttributeKeyValuePairs(cluster.getClusterId(), attributeKeys);
        cluster.setAttributeKeyValues(attributeKeyValues);
        return cluster;

    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED, readOnly = false, rollbackFor = { ValidationError.class,
            ConstraintViolationException.class, DataIntegrityViolationException.class })
    public Cluster addCluster(final Cluster cluster) throws ValidationError {
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final ValidationError v = new ValidationError();
        Cluster insertedCluster = new Cluster();
        final long clusterId;
        try {
            // this.userUtil.validateUser(createdUser);
            cluster.setUpdatedUser(cluster.getCreatedUser());
            cluster.setCreatedTimestamp(sdf.format(timestamp));
            cluster.setUpdatedTimestamp(sdf.format(timestamp));
            cluster.setIsActiveYN(ActiveEnumeration.YES.getFlag());
            insertedCluster = this.clusterRepository.save(cluster);
            clusterId = insertedCluster.getClusterId();

        }
        catch (final ConstraintViolationException e) {
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("Cluster name is empty");
            throw v;
        }
        catch (final DataIntegrityViolationException e) {
            v.setErrorCode(HttpStatus.CONFLICT);
            v.setErrorDescription("Cluster name is duplicated");
            throw v;
        }

        // insert into pc_storage_cluster_attribute_value table
        if (cluster.getAttributeKeyValues() != null && cluster.getAttributeKeyValues().size() > 0) {
            final List<ClusterAttributeKeyValue> attributeKeyValues = cluster.getAttributeKeyValues();
            final List<ClusterAttributeValue> attributeValues = new ArrayList<ClusterAttributeValue>();
            try {
                attributeKeyValues.forEach(attributeKeyValue -> {
                    final ClusterAttributeValue attributeValue = new ClusterAttributeValue(
                            attributeKeyValue.getClusterAttributeKeyId(), clusterId,
                            attributeKeyValue.getClusterAttributeValue(), cluster.getCreatedUser(),
                            sdf.format(timestamp), cluster.getCreatedUser(), sdf.format(timestamp));
                    attributeValues.add(attributeValue);
                });
                this.cavr.saveAll(attributeValues);
            }
            catch (final ConstraintViolationException e) {
                v.setErrorCode(HttpStatus.BAD_REQUEST);
                v.setErrorDescription("Cluster AttributeValue is empty");
                throw v;
            }
            catch (final DataIntegrityViolationException e) {
                v.setErrorCode(HttpStatus.CONFLICT);
                v.setErrorDescription("Invalid Cluster ID or Invalid Cluster Attribute Key ID");
                throw v;
            }
        }
        return insertedCluster;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED, readOnly = false, rollbackFor = { ValidationError.class,
            ConstraintViolationException.class, DataIntegrityViolationException.class,
            TransactionSystemException.class })
    public Cluster updateCluster(final Cluster cluster) throws ValidationError {
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final ValidationError v = new ValidationError();
        Cluster tempCluster = this.clusterUtil.validateCluster(cluster.getClusterId());
        long clusterId;
        try {
            tempCluster.setUpdatedUser(cluster.getCreatedUser());
            tempCluster.setUpdatedTimestamp(sdf.format(timestamp));
            this.s1.setNextChain(this.s2);
            this.s1.validate(cluster, tempCluster);
            tempCluster = this.clusterRepository.save(tempCluster);
            clusterId = tempCluster.getClusterId();
        }
        catch (final TransactionSystemException e) {
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("Cluster name is empty");
            throw v;
        }
        catch (final DataIntegrityViolationException e) {
            v.setErrorCode(HttpStatus.CONFLICT);
            v.setErrorDescription("Cluster name is duplicated");
            throw v;
        }

        if (cluster.getAttributeKeyValues() != null && cluster.getAttributeKeyValues().size() > 0) {
            final List<ClusterAttributeKeyValue> attributeKeyValues = cluster.getAttributeKeyValues();
            final List<ClusterAttributeValue> attributeValues = new ArrayList<ClusterAttributeValue>();
            try {
                attributeKeyValues.forEach(attributeKeyValue -> {
                    if (attributeKeyValue.getClusterAttributeValueId() == 0) {
                        final ClusterAttributeValue attributeValue = new ClusterAttributeValue(
                                attributeKeyValue.getClusterAttributeKeyId(), clusterId,
                                attributeKeyValue.getClusterAttributeValue(), cluster.getCreatedUser(),
                                sdf.format(timestamp), cluster.getCreatedUser(), sdf.format(timestamp));
                        attributeValues.add(attributeValue);
                    }
                    else {
                        final ClusterAttributeValue attributeValue = new ClusterAttributeValue(
                                attributeKeyValue.getClusterAttributeValueId(),
                                attributeKeyValue.getClusterAttributeKeyId(), clusterId,
                                attributeKeyValue.getClusterAttributeValue(), cluster.getCreatedUser(),
                                sdf.format(timestamp), cluster.getCreatedUser(), sdf.format(timestamp));
                        attributeValues.add(attributeValue);
                    }
                });
                this.cavr.saveAll(attributeValues);
            }
            catch (final ConstraintViolationException e) {
                v.setErrorCode(HttpStatus.BAD_REQUEST);
                v.setErrorDescription("Cluster AttributeValue is empty");
                throw v;
            }
            catch (final DataIntegrityViolationException e) {
                v.setErrorCode(HttpStatus.CONFLICT);
                v.setErrorDescription("Invalid Cluster ID or Invalid Cluster Attribute Key ID");
                throw v;
            }
        }
        return tempCluster;
    }

    @Override
    public Cluster deActivateCluster(final long clusterId) throws ValidationError {
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        Cluster tempCluster = this.clusterUtil.validateCluster(clusterId);
        tempCluster.setUpdatedTimestamp(sdf.format(timestamp));
        tempCluster.setIsActiveYN(ActiveEnumeration.NO.getFlag());
        tempCluster = this.clusterRepository.save(tempCluster);
        return tempCluster;

    }

    @Override
    public Cluster getClusterByName(final String clusterName) {
        final List<ClusterAttributeKey> attributeKeys = this.clusterUtil.getAllAttributeKeys();
        final Cluster cluster = this.clusterRepository.findByClusterName(clusterName);
        final List<ClusterAttributeKeyValue> attributeKeyValues = this.clusterUtil
                .getAttributeKeyValuePairs(cluster.getClusterId(), attributeKeys);
        cluster.setAttributeKeyValues(attributeKeyValues);
        return cluster;
    }

    @Override
    public Cluster reActivateCluster(final long clusterId) throws ValidationError {
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        Cluster tempCluster = this.clusterUtil.validateCluster(clusterId);
        tempCluster.setUpdatedTimestamp(sdf.format(timestamp));
        tempCluster.setIsActiveYN(ActiveEnumeration.YES.getFlag());
        tempCluster = this.clusterRepository.save(tempCluster);
        return tempCluster;

    }

}
