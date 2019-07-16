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

package com.paypal.udc.dao.objectschema;

import java.util.List;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.repository.PagingAndSortingRepository;
import com.paypal.udc.entity.objectschema.ObjectSchemaMap;


public interface PageableObjectSchemaMapRepository extends PagingAndSortingRepository<ObjectSchemaMap, Long> {

    @Override
    public Page<ObjectSchemaMap> findAll(Pageable pageable);

    public Page<ObjectSchemaMap> findByStorageSystemIdIn(final List<Long> storageSystemIds, Pageable pageable);

    public Page<ObjectSchemaMap> findByStorageSystemIdInAndObjectNameContaining(final List<Long> storageSystemIds,
            final String objectStr, Pageable pageable);

    public Page<ObjectSchemaMap> findByStorageSystemIdAndContainerName(final long storageSystemId,
            final String containerName, Pageable pageable);

    public Page<ObjectSchemaMap> findByStorageSystemId(final long storageSystemId, Pageable pageable);

    public Page<ObjectSchemaMap> findByStorageSystemIdAndObjectNameContaining(final long storageSystemId,
            final String objectStr, Pageable pageable);

    public Page<ObjectSchemaMap> findByStorageSystemIdAndIsSelfDiscovered(final long storageSystemId,
            final String isSelfDiscovered, Pageable pageable);

    public Page<ObjectSchemaMap> findByContainerName(final String containerName, Pageable pageable);

    public Page<ObjectSchemaMap> findByContainerNameAndStorageSystemIdIn(final List<Long> storageSystemIds,
            final String containerName, Pageable pageable);

    public Page<ObjectSchemaMap> findByStorageSystemIdInAndIsActiveYN(final List<Long> storageSystemIds,
            final String isActiveYN, Pageable pageable);

    public Page<ObjectSchemaMap> findByStorageSystemId(final long storageSystemId, final String isActiveYN,
            Pageable pageable);

    public Page<ObjectSchemaMap> findByStorageSystemIdAndIsActiveYNAndIsRegisteredAndIsSelfDiscovered(
            final long storageSystemId, final String isActiveYN, final String isRegistered,
            final String isSelfDiscovered, Pageable pageable);
}
