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

package com.paypal.udc.util;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import com.google.gson.Gson;
import com.paypal.udc.dao.storagecategory.ElasticSearchStorageRepository;
import com.paypal.udc.dao.storagecategory.StorageRepository;
import com.paypal.udc.entity.storagecategory.ElasticSearchStorage;
import com.paypal.udc.entity.storagecategory.Storage;
import com.paypal.udc.exception.ValidationError;


@Component
public class StorageUtil {

    @Autowired
    private StorageRepository storageCache;
    @Autowired
    private ElasticSearchStorageRepository esStoreRepository;
    @Autowired
    private TransportClient client;

    public Storage validateStorageId(final long id) throws ValidationError {
        final Storage storage = this.storageCache.findById(id).orElse(null);
        if (storage == null) {
            final ValidationError verror = new ValidationError();
            verror.setErrorCode(HttpStatus.BAD_REQUEST);
            verror.setErrorDescription("Invalid Storage ID");
            throw verror;
        }
        return storage;
    }

    public void upsertStorage(final String esStorageCategoryIndex, final String esType, final Storage storage,
            final ElasticsearchTemplate esTemplate) throws IOException, InterruptedException, ExecutionException {

        final List<ElasticSearchStorage> esStorageCategory = this.esStoreRepository
                .findByStorageId(storage.getStorageId());

        if (esStorageCategory == null || esStorageCategory.isEmpty()) {
            this.save(esStorageCategoryIndex, esType, storage);
            esTemplate.refresh(esStorageCategoryIndex);
        }
        else {
            this.update(esStorageCategoryIndex, esType, esStorageCategory.get(0), storage);
            esTemplate.refresh(esStorageCategoryIndex);
        }

    }

    public void save(final String indexName, final String typeName, final Storage storage) {

        final Gson gson = new Gson();

        final ElasticSearchStorage esStorage = new ElasticSearchStorage(storage.getStorageId(),
                storage.getStorageName(), storage.getStorageDescription(), storage.getIsActiveYN(),
                storage.getCreatedUser(), storage.getCreatedTimestamp(), storage.getUpdatedUser(),
                storage.getUpdatedTimestamp());

        final IndexResponse response = this.client.prepareIndex(indexName, typeName, esStorage.get_id())
                .setSource(gson.toJson(esStorage), XContentType.JSON)
                .get();
    }

    public void update(final String indexName, final String typeName, final ElasticSearchStorage esStorage,
            final Storage storage) throws IOException, InterruptedException, ExecutionException {

        final Gson gson = new Gson();

        final UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index(indexName).type(typeName)
                .id(esStorage.get_id())
                .doc(gson.toJson(storage), XContentType.JSON);

        final UpdateResponse updateResponse = this.client.update(updateRequest).get();
    }
}
