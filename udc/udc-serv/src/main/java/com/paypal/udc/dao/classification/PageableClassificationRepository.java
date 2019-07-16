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

package com.paypal.udc.dao.classification;

import java.util.List;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.data.repository.query.Param;
import com.paypal.udc.entity.classification.Classification;


public interface PageableClassificationRepository
        extends PagingAndSortingRepository<Classification, Long> {

    Page<Classification> findByClassificationId(final Pageable pageable, long classificationId);

    @Query(value = "select p.*,m.storage_dataset_id,m.storage_system_id,m.entity_id,m.zone_id from pc_classifications p inner join pc_classifications_map m  on p.dataset_classification_id=m.dataset_classification_id"
            + " where provider_id in :providerIds and classification_id in :classIds and m.storage_system_id in :systemIds and m.entity_id in :entityIds and m.zone_id in :zoneIds", countQuery = "select count(*) from (select p.*,m.storage_dataset_id,m.storage_system_id,m.entity_id,m.zone_id from pc_classifications p inner join pc_classifications_map m  on p.dataset_classification_id=m.dataset_classification_id "
                    + "where provider_id in :providerIds and classification_id in :classIds and m.storage_system_id in :systemIds and m.entity_id in :entityIds and m.zone_id in :zoneIds) counts", nativeQuery = true)
    Page<Object[]> findByMiscAttributes(@Param("providerIds") final List<Long> providerIds,
            @Param("classIds") final List<Long> classIds, @Param("systemIds") final List<Long> systemIds,
            @Param("entityIds") final List<Long> entityIds, @Param("zoneIds") final List<Long> zoneIds,
            Pageable pageable);

}
