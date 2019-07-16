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

package com.paypal.udc.service;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import com.paypal.udc.entity.storagesystem.StorageSystem;
import com.paypal.udc.entity.storagesystem.StorageSystemAttributeValue;
import com.paypal.udc.entity.storagesystem.StorageSystemDiscovery;
import com.paypal.udc.exception.ValidationError;


public interface IStorageSystemService {

    List<StorageSystem> getAllStorageSystems();

    StorageSystem getStorageSystemById(long storageSystemId) throws ValidationError;

    StorageSystem addStorageSystem(StorageSystem storageSystem)
            throws ValidationError, IOException, InterruptedException, ExecutionException;

    StorageSystem updateStorageSystem(StorageSystem storageSystem)
            throws ValidationError, IOException, InterruptedException, ExecutionException;

    StorageSystem deleteStorageSystem(long storageSystemId)
            throws ValidationError, IOException, InterruptedException, ExecutionException;

    List<StorageSystem> getStorageSystemByStorageType(long storageTypeId);

    List<StorageSystemAttributeValue> getStorageSystemAttributes(Long storageSystemId);

    List<StorageSystemAttributeValue> getAttributeValuesByName(String storageSystemName) throws ValidationError;

    List<StorageSystem> getStorageSystemByType(String name) throws ValidationError;

    StorageSystem enableStorageSystem(long id)
            throws ValidationError, IOException, InterruptedException, ExecutionException;

    StorageSystemDiscovery addStorageSystemDiscovery(StorageSystemDiscovery storageSystemDiscovery)
            throws ValidationError, ParseException;

    List<StorageSystemDiscovery> getDiscoveryStatusForStorageSystemId(final String storageSystemList);

    List<StorageSystem> getStorageSystemByZoneAndType(String zoneName, String typeName) throws ValidationError;

}
