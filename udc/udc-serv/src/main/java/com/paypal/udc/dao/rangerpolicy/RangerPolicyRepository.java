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

package com.paypal.udc.dao.rangerpolicy;

import java.util.List;
import org.springframework.data.repository.CrudRepository;
import com.paypal.udc.entity.rangerpolicy.DerivedPolicy;


public interface RangerPolicyRepository extends CrudRepository<DerivedPolicy, Long> {

    List<DerivedPolicy> findByPolicyLocationsAndTypeNameAndClusterIdAndIsActiveYN(String location, String typeName,
            long clusterId, String isActiveYN);

    List<DerivedPolicy> findByTableAndTypeNameAndClusterIdAndIsActiveYN(String table, String typeName,
            long clusterId, String isActiveYN);

    DerivedPolicy findByClusterIdAndPolicyId(long clusterId, int policyId);

    List<DerivedPolicy> findByClusterIdAndIsActiveYN(long clusterId, String isActiveYN);

    List<DerivedPolicy> findByPolicyLocationsContainingAndTypeNameAndClusterId(String tempLoc, String type,
            long clusterId);

    List<DerivedPolicy> findByTableContainingAndTypeNameAndClusterId(String table, String type,
            long clusterId);

    List<DerivedPolicy> findByTableAndDatabaseAndTypeNameAndClusterIdAndIsActiveYN(String database, String table,
            String type, long clusterId, String isActiveYN);

    List<DerivedPolicy> findByDatabaseAndTypeNameAndClusterIdAndIsActiveYN(String database, String type,
            long clusterId, String isActiveYN);

}
