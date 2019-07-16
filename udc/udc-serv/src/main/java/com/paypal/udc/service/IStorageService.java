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
import com.paypal.udc.entity.storagecategory.Storage;
import com.paypal.udc.exception.ValidationError;


public interface IStorageService {

    List<Storage> getAllStorages();

    Storage getStorageById(long storageId) throws ValidationError;

    Storage addStorage(Storage storage) throws ValidationError, IOException, InterruptedException, ExecutionException;

    Storage updateStorage(Storage storage)
            throws ValidationError, IOException, InterruptedException, ExecutionException;

    Storage deleteStorage(long storageId) throws ValidationError, IOException, InterruptedException, ExecutionException;

    Storage getStorageByName(String name);

    Storage enableStorage(long id) throws ValidationError, IOException, InterruptedException, ExecutionException;
}
