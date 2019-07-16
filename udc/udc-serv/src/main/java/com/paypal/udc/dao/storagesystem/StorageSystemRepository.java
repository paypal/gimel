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

package com.paypal.udc.dao.storagesystem;

import java.util.List;
import org.springframework.data.repository.CrudRepository;
import com.paypal.udc.entity.storagesystem.StorageSystem;


public interface StorageSystemRepository extends CrudRepository<StorageSystem, Long> {

    public List<StorageSystem> findByZoneId(long zoneId);

    public List<StorageSystem> findByZoneIdAndStorageTypeId(long zoneId, long storageTypeId);

    public List<StorageSystem> findByStorageTypeId(long storageTypeId);

    public List<StorageSystem> findByStorageSystemIdIn(final List<Long> systemIds);

    public StorageSystem findByStorageSystemName(String storageSystemName);

    public List<StorageSystem> findByIsActiveYN(final String isActiveYN);

    public List<StorageSystem> findByStorageTypeIdIn(List<Long> typeIds);

    public List<StorageSystem> findByStorageSystemNameIn(List<String> systems);

}
