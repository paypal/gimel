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
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import com.paypal.udc.entity.dataset.Dataset;
import com.paypal.udc.entity.objectschema.CollectiveObjectSchemaMap;
import com.paypal.udc.entity.objectschema.ObjectAttributeKeyValue;
import com.paypal.udc.entity.objectschema.ObjectSchemaMap;
import com.paypal.udc.exception.ValidationError;


public interface IObjectSchemaMapService {

    Page<CollectiveObjectSchemaMap> getPagedObjectMappings(final long storageSystemId, final Pageable pageable);

    ObjectSchemaMap getDatasetById(long topicId) throws ValidationError;

    ObjectSchemaMap addObjectSchema(ObjectSchemaMap topic)
            throws ValidationError, IOException, InterruptedException, ExecutionException;

    List<String> getDistinctContainerNamesByStorageSystemId(final long storageSystemId);

    List<String> getDistinctObjectNames(final String containerName, final long storageSystemId);

    // List<CollectiveObjectSchemaMap> getUnRegisteredObjects(final long systemId);

    Page<CollectiveObjectSchemaMap> getPagedUnRegisteredObjects(final long systemId, final Pageable pageable);

    ObjectSchemaMap updateObjectSchemaMap(final ObjectSchemaMap schemaMap)
            throws ValidationError, IOException, InterruptedException, ExecutionException;

    List<Dataset> getDatasetBySystemContainerAndObject(String systemName, String containerName, String objectName)
            throws ValidationError;

    List<ObjectSchemaMap> getObjectSchemaMapsBySystemIds(final long storageSystemId);

    List<CollectiveObjectSchemaMap> getSchemaBySystemContainerAndObject(final long systemId, String containerName,
            String objectName);

    void deActivateObjectAndDataset(final long objectId) throws ValidationError;

    List<String> getDistinctContainerNames();

    Page<ObjectSchemaMap> getObjectsByStorageSystemAndContainer(String storageSystemName, String containerName,
            final Pageable pageable);

    Page<ObjectSchemaMap> getObjectsByStorageSystem(final String objectStr, String storageSystemName,
            final Pageable pageable);

    ObjectAttributeKeyValue addObjectAttributeKeyValue(ObjectAttributeKeyValue topic)
            throws ValidationError, IOException, InterruptedException, ExecutionException;

    ObjectAttributeKeyValue updateObjectAttributeKeyValue(ObjectAttributeKeyValue topics)
            throws ValidationError, IOException, InterruptedException, ExecutionException;

    List<ObjectAttributeKeyValue> getCustomAttributesByObject(long objectId);

    List<Dataset> getDatasetByObject(String objectName);

    List<String> getDistinctContainerNamesBySystems(String systemList);

}
