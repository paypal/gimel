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
import java.util.Set;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import com.paypal.udc.entity.objectschema.ObjectSchemaMap;


public interface ObjectSchemaMapRepository extends CrudRepository<ObjectSchemaMap, Long> {

    public List<ObjectSchemaMap> findByObjectIdIn(final Set<Long> objectIds);

    public List<ObjectSchemaMap> findByObjectName(final String dataSetName);

    @Query(value = "select * from pc_object_schema_map where object_id in "
            + "(select max(object_id) from pc_object_schema_map where storage_system_id=:storageSystemId group by object_name)", nativeQuery = true)
    public List<ObjectSchemaMap> findAllTopics(@Param("storageSystemId") final long storageSystemId);

    @Query(value = "select distinct(container_name) from pc_object_schema_map where storage_system_id=:storageSystemId", nativeQuery = true)
    public List<String> findAllContainerNamesByStorageSystemId(@Param("storageSystemId") final long storageSystemId);

    @Query(value = "select distinct(object_name) from pc_object_schema_map where container_name=:containerName and storage_system_id=:storageSystemId", nativeQuery = true)
    public List<String> findAllObjectNames(@Param("containerName") final String containerName,
            @Param("storageSystemId") final long storageSystemId);

    @Query(value = "select * from pc_object_schema_map where container_name=BINARY :containerName and storage_system_id=:systemId and object_name= BINARY :objectName", nativeQuery = true)
    public ObjectSchemaMap findByStorageSystemIdAndContainerNameAndObjectName(
            @Param("systemId") final long systemId,
            @Param("containerName") final String containerName, @Param("objectName") final String objectName);

    public List<ObjectSchemaMap> findByStorageSystemIdIn(final List<Long> storageSystemIds);

    public List<ObjectSchemaMap> findByStorageSystemId(final long storageSystemId);

    @Query(value = "select distinct LOWER(container_name) from pc_object_schema_map", nativeQuery = true)
    public List<String> findAllContainerNames();

    @Query(value = "select distinct LOWER(container_name) from pc_object_schema_map where storage_system_id in (:systemIds) and is_active_y_n='Y'", nativeQuery = true)
    public List<String> findAllContainerNamesBySystemIdIn(@Param("systemIds") List<Long> systemIds);

    public List<ObjectSchemaMap> findByStorageSystemIdInAndIsActiveYN(final List<Long> storageSystemIds,
            final String isActiveYN);

    public Page<ObjectSchemaMap> findByStorageSystemIdAndIsActiveYN(final long storageSystemId,
            final String isActiveYN, final Pageable pageable);

    public long countByStorageSystemId(final long storageSystemId);

    public long countByStorageSystemIdAndIsActiveYNAndIsRegistered(final long storageSystemId, final String isActiveYN,
            final String isRegistered);

    public List<ObjectSchemaMap> findByStorageSystemIdAndIsRegistered(long systemId, String isRegistered);

    public List<ObjectSchemaMap> findByObjectNameContaining(String objectName);

}
