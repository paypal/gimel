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
import java.util.List;
import java.util.concurrent.ExecutionException;
import com.paypal.udc.entity.storagetype.CollectiveStorageTypeAttributeKey;
import com.paypal.udc.entity.storagetype.StorageType;
import com.paypal.udc.entity.storagetype.StorageTypeAttributeKey;
import com.paypal.udc.exception.ValidationError;


public interface IStorageTypeService {

    List<StorageType> getAllStorageTypes();

    StorageType getStorageTypeById(long storageTypeId) throws ValidationError;

    StorageType addStorageType(StorageType storageType)
            throws ValidationError, IOException, InterruptedException, ExecutionException;

    StorageType updateStorageType(StorageType storageType)
            throws ValidationError, IOException, InterruptedException, ExecutionException;

    StorageType deleteStorageType(long storageTypeId)
            throws ValidationError, IOException, InterruptedException, ExecutionException;

    List<StorageType> getStorageTypeByStorageCategory(long storageId);

    List<StorageType> getStorageTypeByStorageCategoryName(String storageName);

    List<StorageTypeAttributeKey> getStorageAttributeKeys(final long storageTypeId, final String isStorageSystemLevel);

    StorageTypeAttributeKey updateStorageTypeAttributeKeys(StorageTypeAttributeKey attributeKey);

    StorageTypeAttributeKey insertStorageTypeAttributeKey(CollectiveStorageTypeAttributeKey attributeKey)
            throws ValidationError;

    void deleteStorageAttributeKey(long storageTypeId, String storageAttributeKeys);

    StorageType enableStorageType(long id) throws ValidationError;

    List<StorageTypeAttributeKey> getAllStorageAttributeKeys(long storageTypeId);

}
