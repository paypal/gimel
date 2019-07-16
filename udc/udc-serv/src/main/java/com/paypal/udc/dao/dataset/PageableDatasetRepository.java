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

package com.paypal.udc.dao.dataset;

import java.util.List;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.repository.PagingAndSortingRepository;
import com.paypal.udc.entity.dataset.Dataset;


public interface PageableDatasetRepository extends PagingAndSortingRepository<Dataset, Long> {

    public Page<Dataset> findByStorageDataSetIdInAndStorageDataSetNameContaining(final List<Long> datasetIds,
            String datasetStr, Pageable pageable);

    public Page<Dataset> findByIsActiveYNAndStorageDataSetNameContaining(final String isActiveYN,
            final String datasetStr, Pageable pageable);

    public Page<Dataset> findByIsActiveYNAndStorageDataSetNameContainingAndStorageSystemIdIn(final String isActiveYN,
            final String datasetStr, Pageable pageable, final List<Long> objectIds);

    public Page<Dataset> findByIsActiveYNAndStorageSystemIdIn(final String isActiveYN, final List<Long> objectIds,
            Pageable pageable);

    public Page<Dataset> findByIsActiveYNAndStorageSystemId(final String isActiveYN, final long storageSystemId,
            final Pageable pageable);

    public Page<Dataset> findByIsActiveYNAndStorageDataSetNameContainingAndStorageSystemId(final String isActiveYN,
            final String datasetStr, final long storageSystemId, final Pageable pageable);

}
