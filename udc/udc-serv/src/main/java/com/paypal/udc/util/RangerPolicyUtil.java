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
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import com.paypal.udc.dao.rangerpolicy.PolicyDiscoveryMetricRepository;
import com.paypal.udc.dao.rangerpolicy.RangerPolicyRepository;
import com.paypal.udc.dao.rangerpolicy.RangerPolicyUserGroupRepository;
import com.paypal.udc.entity.rangerpolicy.DerivedPolicy;
import com.paypal.udc.entity.rangerpolicy.DerivedPolicyItem;
import com.paypal.udc.entity.rangerpolicy.PolicyDiscoveryMetric;
import com.paypal.udc.exception.ValidationError;
import com.paypal.udc.util.enumeration.ActiveEnumeration;
import com.paypal.udc.util.enumeration.PolicyTypeEnumeration;


@Component
public class RangerPolicyUtil {

    final static Logger logger = LoggerFactory.getLogger(RangerPolicyUtil.class);
    @Autowired
    private RangerPolicyRepository rangerPolicyRepository;
    @Autowired
    private RangerPolicyUserGroupRepository rangerPolicyUserGroupRepository;
    @Autowired
    private PolicyDiscoveryMetricRepository discoveryMetricRepository;

    public List<DerivedPolicy> computePoliciesByLocation(final String location, final String type,
            final long clusterId, final String table, final String database) {
        final List<DerivedPolicy> derivedPolicies = new ArrayList<DerivedPolicy>();
        switch (type) {
            case "hdfs": {

                String tempLoc = location;
                while (derivedPolicies.size() == 0 && tempLoc.chars().filter(ch -> ch == '/').count() > 1) {
                    derivedPolicies.addAll(this.rangerPolicyRepository
                            .findByPolicyLocationsAndTypeNameAndClusterIdAndIsActiveYN(tempLoc, type, clusterId,
                                    ActiveEnumeration.YES.getFlag()));
                    if (tempLoc.contains("/")) {
                        tempLoc = tempLoc.substring(0, tempLoc.lastIndexOf("/"));
                    }

                    if (derivedPolicies != null && derivedPolicies.size() > 0) {
                        derivedPolicies.forEach(policy -> {
                            final long derivedPolicyId = policy.getDerivedPolicyId();
                            final List<DerivedPolicyItem> policyItems = this.rangerPolicyUserGroupRepository
                                    .findByDerivedPolicyId(derivedPolicyId);
                            policy.setPolicyItems(policyItems);
                        });
                        break;
                    }
                }
                break;
            }
            case "hbase": {
                final String tempLoc = table;
                final List<DerivedPolicy> currentPolicies = new ArrayList<DerivedPolicy>();
                final List<DerivedPolicy> tablePolicies = this.rangerPolicyRepository
                        .findByTableAndTypeNameAndClusterIdAndIsActiveYN(tempLoc, type, clusterId,
                                ActiveEnumeration.YES.getFlag());
                if (tablePolicies != null && tablePolicies.size() > 0) {
                    currentPolicies.addAll(tablePolicies);
                }
                if (tempLoc.contains(":")) {
                    final String[] namespaceTable = tempLoc.split(":");
                    final List<DerivedPolicy> namespacePolicies = this.rangerPolicyRepository
                            .findByTableAndTypeNameAndClusterIdAndIsActiveYN(namespaceTable[0] + ":*", type, clusterId,
                                    ActiveEnumeration.YES.getFlag());
                    currentPolicies.addAll(namespacePolicies);
                }

                if (currentPolicies != null && currentPolicies.size() > 0) {
                    currentPolicies.forEach(policy -> {
                        final long derivedPolicyId = policy.getDerivedPolicyId();
                        final List<DerivedPolicyItem> policyItems = this.rangerPolicyUserGroupRepository
                                .findByDerivedPolicyId(derivedPolicyId);
                        policy.setPolicyItems(policyItems);
                    });
                }
                derivedPolicies.addAll(currentPolicies);
                break;
            }

            case "hive": {
                List<DerivedPolicy> currentPolicies = this.rangerPolicyRepository
                        .findByTableAndDatabaseAndTypeNameAndClusterIdAndIsActiveYN(database, table, type, clusterId,
                                ActiveEnumeration.YES.getFlag());

                if (currentPolicies.size() == 0) {
                    currentPolicies = this.rangerPolicyRepository
                            .findByDatabaseAndTypeNameAndClusterIdAndIsActiveYN(database, type, clusterId,
                                    ActiveEnumeration.YES.getFlag());
                }
                if (currentPolicies != null && currentPolicies.size() > 0) {
                    currentPolicies.forEach(policy -> {
                        final long derivedPolicyId = policy.getDerivedPolicyId();
                        final List<DerivedPolicyItem> policyItems = this.rangerPolicyUserGroupRepository
                                .findByDerivedPolicyId(derivedPolicyId);
                        policy.setPolicyItems(policyItems);
                    });

                }
                derivedPolicies.addAll(currentPolicies);
                break;
            }
        }
        return derivedPolicies;

    }

    public void validatePolicyDiscoveryType(final String policyDiscoveryMetricType) throws ValidationError {
        final List<String> typeList = Arrays.asList(PolicyTypeEnumeration.values()).stream()
                .map(type -> type.getFlag()).collect(Collectors.toList());

        if (!typeList.contains(policyDiscoveryMetricType)) {
            final ValidationError verror = new ValidationError();
            verror.setErrorCode(HttpStatus.BAD_REQUEST);
            verror.setErrorDescription("Invalid Type Supplied");
            throw verror;
        }
    }

    public List<PolicyDiscoveryMetric> getAllDiscoveryMetricsByType(final String discoveryType) {
        final List<PolicyDiscoveryMetric> discoveryMetrics = this.discoveryMetricRepository
                .findByPolicyDiscoveryMetricType(discoveryType);
        return discoveryMetrics;
    }
}
