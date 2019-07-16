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
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import javax.validation.ConstraintViolationException;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import com.google.common.collect.Lists;
import com.paypal.udc.dao.rangerpolicy.PolicyDiscoveryMetricRepository;
import com.paypal.udc.dao.rangerpolicy.RangerPolicyRepository;
import com.paypal.udc.dao.rangerpolicy.RangerPolicyUserGroupRepository;
import com.paypal.udc.entity.rangerpolicy.DerivedPolicy;
import com.paypal.udc.entity.rangerpolicy.DerivedPolicyItem;
import com.paypal.udc.entity.rangerpolicy.PolicyDiscoveryMetric;
import com.paypal.udc.exception.ValidationError;
import com.paypal.udc.service.IRangerService;
import com.paypal.udc.util.ClusterUtil;
import com.paypal.udc.util.RangerPolicyUtil;
import com.paypal.udc.util.enumeration.ActiveEnumeration;


@Service
@Transactional(propagation = Propagation.SUPPORTS, readOnly = true)
public class RangerService implements IRangerService {

    final static Logger logger = Logger.getLogger(RangerService.class);
    @Autowired
    private RangerPolicyRepository rangerPolicyRepository;
    @Autowired
    private RangerPolicyUserGroupRepository rangerPolicyUserGroupRepository;
    @Autowired
    private ClusterUtil clusterUtil;
    @Autowired
    private RangerPolicyUtil rangerUtil;
    @Autowired
    private PolicyDiscoveryMetricRepository discoveryMetricRepository;

    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss");
    private static final String user = "udc_admin";

    @Override
    public List<DerivedPolicy> getAllPolicies(final long clusterId) {

        final List<DerivedPolicy> derivedPolicies = new ArrayList<DerivedPolicy>();
        this.rangerPolicyRepository.findByClusterIdAndIsActiveYN(clusterId, ActiveEnumeration.YES.getFlag())
                .forEach(policy -> {
                    final long derivedPolicyId = policy.getDerivedPolicyId();
                    final List<DerivedPolicyItem> policyItems = this.rangerPolicyUserGroupRepository
                            .findByDerivedPolicyId(derivedPolicyId);
                    policy.setPolicyItems(policyItems);
                    derivedPolicies.add(policy);
                });
        return derivedPolicies;
    }

    @Override
    public List<DerivedPolicy> getPolicyByPolicyLocations(final String location, final String type,
            final long clusterId, final String table, final String database) {

        final List<DerivedPolicy> policies = new ArrayList<DerivedPolicy>();
        if (location != null && location.contains(",")) {
            final List<String> locations = Arrays.asList(location.trim().split(","));
            locations.forEach(tempLoc -> {
                final List<DerivedPolicy> currentPolicies = this.rangerUtil.computePoliciesByLocation(tempLoc.trim(),
                        type, clusterId, table, database);
                policies.addAll(currentPolicies);
            });
            return policies.stream().distinct().collect(Collectors.toList());
        }
        else {
            return this.rangerUtil.computePoliciesByLocation(location, type, clusterId, table, database);
        }
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED, readOnly = false, rollbackFor = {
            TransactionSystemException.class, DataIntegrityViolationException.class, IndexOutOfBoundsException.class,
            ValidationError.class })
    public void updatePolicy(final DerivedPolicy policy) throws ValidationError {
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final String time = sdf.format(timestamp);
        DerivedPolicy tempPolicy = new DerivedPolicy();
        final ValidationError v = new ValidationError();
        if (policy.getDerivedPolicyId() == 0) {
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("Not valid for Update.");
            throw v;
        }
        try {
            this.clusterUtil.validateCluster(policy.getClusterId());
            tempPolicy = new DerivedPolicy(policy.getDerivedPolicyId(), policy.getPolicyId(), policy.getClusterId(),
                    policy.getPolicyName(), policy.getTypeName(), policy.getPolicyLocations(), policy.getColumn(),
                    policy.getColumnFamily(), policy.getDatabase(), policy.getTable(), policy.getQueue(),
                    ActiveEnumeration.YES.getFlag(), user, time, user, time);
            tempPolicy = this.rangerPolicyRepository.save(tempPolicy);
        }
        catch (final DataIntegrityViolationException e) {
            v.setErrorCode(HttpStatus.CONFLICT);
            v.setErrorDescription("Cluster Id and policy ID are duplicated");
            throw v;
        }
        catch (final ConstraintViolationException e) {
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("Policy name is empty");
            throw v;
        }
        final long derivedPolicyId = tempPolicy.getDerivedPolicyId();
        final List<DerivedPolicyItem> policyItems = policy.getPolicyItems();

        if (policyItems != null && policy.getPolicyItems().size() > 0) {
            final List<DerivedPolicyItem> currentPolicyItems = this.rangerPolicyUserGroupRepository
                    .findByDerivedPolicyId(derivedPolicyId);
            this.rangerPolicyUserGroupRepository.deleteAll(currentPolicyItems);
            policyItems.forEach(policyItem -> {
                final DerivedPolicyItem dpi = new DerivedPolicyItem(derivedPolicyId,
                        policyItem.getAccessTypes(), policyItem.getUsers(), policyItem.getGroups(),
                        ActiveEnumeration.YES.getFlag(), user, time, user, time);
                this.rangerPolicyUserGroupRepository.save(dpi);
            });
        }
    }

    @Override
    public PolicyDiscoveryMetric addPolicyDiscoveryStatus(final PolicyDiscoveryMetric policyStatus)
            throws ValidationError {
        this.rangerUtil.validatePolicyDiscoveryType(policyStatus.getPolicyDiscoveryMetricType());
        return this.discoveryMetricRepository.save(policyStatus);

    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED, readOnly = false, rollbackFor = { ValidationError.class,
            ConstraintViolationException.class, DataIntegrityViolationException.class })
    public DerivedPolicy addPolicy(final DerivedPolicy policy) throws ValidationError {
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final String time = sdf.format(timestamp);
        DerivedPolicy tempPolicy = new DerivedPolicy();
        final ValidationError v = new ValidationError();

        try {
            this.clusterUtil.validateCluster(policy.getClusterId());
            policy.setIsActiveYN(ActiveEnumeration.YES.getFlag());
            tempPolicy = new DerivedPolicy(policy.getPolicyId(), policy.getClusterId(), policy.getPolicyName(),
                    policy.getTypeName(), policy.getPolicyLocations(), policy.getColumn(),
                    policy.getColumnFamily(), policy.getDatabase(), policy.getTable(), policy.getQueue(),
                    ActiveEnumeration.YES.getFlag(), user, time, user, time);
            tempPolicy = this.rangerPolicyRepository.save(tempPolicy);
        }
        catch (final DataIntegrityViolationException e) {
            v.setErrorCode(HttpStatus.CONFLICT);
            v.setErrorDescription("Cluster Id and policy ID are duplicated");
            throw v;
        }
        catch (final ConstraintViolationException e) {
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("Policy name is empty");
            throw v;
        }

        // insert into pc_ranger_policy_user_group
        final long derivedPolicyId = tempPolicy.getDerivedPolicyId();
        final List<DerivedPolicyItem> policyItems = policy.getPolicyItems();
        if (policyItems != null && policy.getPolicyItems().size() > 0) {
            policyItems.forEach(policyItem -> {
                policyItem.setCreatedUser(user);
                policyItem.setUpdatedUser(user);
                policyItem.setCreatedTimestamp(time);
                policyItem.setUpdatedTimestamp(time);
                policyItem.setDerivedPolicyId(derivedPolicyId);
                policyItem.setIsActiveYN(ActiveEnumeration.YES.getFlag());
            });

            final List<DerivedPolicyItem> tempPolicyItems = Lists
                    .newArrayList(this.rangerPolicyUserGroupRepository.saveAll(policyItems));
            tempPolicy.setPolicyItems(tempPolicyItems);
        }
        return tempPolicy;

    }

    @Override
    public DerivedPolicy getPolicyByClusterIdAndPolicyId(final long clusterId, final int policyId) {
        final DerivedPolicy derivedPolicy = this.rangerPolicyRepository.findByClusterIdAndPolicyId(clusterId, policyId);
        final List<DerivedPolicyItem> policyItems = this.rangerPolicyUserGroupRepository
                .findByDerivedPolicyId(derivedPolicy.getDerivedPolicyId());
        if (policyItems == null || policyItems.size() == 0) {
            derivedPolicy.setPolicyItems(new ArrayList<DerivedPolicyItem>());
        }
        else {
            derivedPolicy.setPolicyItems(policyItems);
        }
        return derivedPolicy;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED, readOnly = false, rollbackFor = { ValidationError.class,
            ConstraintViolationException.class, DataIntegrityViolationException.class })
    public void deactivatePolicy(final long derivedPolicyId) throws ValidationError {
        final ValidationError v = new ValidationError();
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final String time = sdf.format(timestamp);
        final DerivedPolicy derivedPolicy = this.rangerPolicyRepository.findById(derivedPolicyId)
                .orElse(null);
        if (derivedPolicy == null) {
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("Derived Policy ID not found");
            throw v;
        }
        derivedPolicy.setUpdatedTimestamp(time);
        derivedPolicy.setUpdatedUser(user);
        derivedPolicy.setIsActiveYN(ActiveEnumeration.NO.getFlag());
        this.rangerPolicyRepository.save(derivedPolicy);

        final List<DerivedPolicyItem> policyItems = this.rangerPolicyUserGroupRepository
                .findByDerivedPolicyId(derivedPolicyId);
        if (policyItems != null && policyItems.size() > 0) {
            policyItems.forEach(policyItem -> {
                policyItem.setUpdatedTimestamp(time);
                policyItem.setUpdatedUser(user);
                policyItem.setIsActiveYN(ActiveEnumeration.NO.getFlag());
            });
            this.rangerPolicyUserGroupRepository.saveAll(policyItems);
        }
    }

    @Override
    public PolicyDiscoveryMetric getRecentPolicyDiscoveryMetric(final String discoveryType) {

        final List<PolicyDiscoveryMetric> discoveryMetrics = this.rangerUtil
                .getAllDiscoveryMetricsByType(discoveryType);

        if (discoveryMetrics.size() > 0) {
            final PolicyDiscoveryMetric discoveryStatus = discoveryMetrics.stream()
                    .max(Comparator.comparing(PolicyDiscoveryMetric::getPolicyDiscoveryMetricId))
                    .get();
            return discoveryStatus;
        }
        else {
            return new PolicyDiscoveryMetric();
        }
    }
}
