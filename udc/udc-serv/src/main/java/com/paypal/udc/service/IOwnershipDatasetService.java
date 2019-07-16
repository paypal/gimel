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
import javax.mail.MessagingException;
import com.paypal.udc.entity.ownership.DatasetOwnershipMap;
import com.paypal.udc.exception.ValidationError;


public interface IOwnershipDatasetService {

    List<DatasetOwnershipMap> getAllOwners();

    List<DatasetOwnershipMap> getOwnersByDatasetId(long datasetId);

    List<DatasetOwnershipMap> addDatasetOwnershipMap(DatasetOwnershipMap datasetOwnershipMap)
            throws ValidationError, MessagingException, IOException, InterruptedException, ExecutionException;

    DatasetOwnershipMap updateDatasetOwnershipMap(DatasetOwnershipMap datasetOwnershipMap) throws ValidationError;

    DatasetOwnershipMap getOwnerBySystemContainerObjectAndOwnerName(final String storageSystemName,
            final String containerName, final String objectName, final String ownerName) throws ValidationError;

    List<DatasetOwnershipMap> getOwnersBySystemContainerAndObject(final String storageSystemName,
            final String containerName, final String objectName) throws ValidationError;

    DatasetOwnershipMap updateDatasetOwnershipNotificationStatus(DatasetOwnershipMap datasetOwnershipMap)
            throws ValidationError;

    List<DatasetOwnershipMap> getAllNewOwners();

}
