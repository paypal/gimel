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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import com.paypal.udc.dao.entity.ElasticSearchEntityRepository;
import com.paypal.udc.dao.entity.EntityRepository;
import com.paypal.udc.entity.entity.ElasticSearchEntity;
import com.paypal.udc.entity.entity.Entity;
import com.paypal.udc.exception.ValidationError;
import com.paypal.udc.service.impl.EntityService;


@Component
public class EntityUtil {

    @Autowired
    private EntityRepository entityRepository;

    @Autowired
    private ElasticSearchEntityRepository esEntityRepository;

    @Autowired
    private TransportClient client;

    @Autowired
    EntityService entityService;

    public Entity validateEntity(final long entityId) throws ValidationError {
        final Entity entity = this.entityRepository.findById(entityId).orElse(null);
        if (entity == null) {
            final ValidationError v = new ValidationError();
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("EntityID is incorrect");
            throw v;
        }
        return entity;
    }

    public Map<Long, Entity> getEntities() {
        final Map<Long, Entity> entities = new HashMap<Long, Entity>();
        this.entityRepository.findAll()
                .forEach(entity -> {
                    entities.put(entity.getEntityId(), entity);
                });
        return entities;
    }

    public void upsertEntities(final String esIndex, final String esType, final Entity entity,
            final ElasticsearchTemplate esTemplate) throws IOException, InterruptedException, ExecutionException {

        final List<ElasticSearchEntity> esEntity = this.esEntityRepository.findByEntityId(entity.getEntityId());
        if (esEntity == null || esEntity.isEmpty()) {
            this.save(esIndex, esType, entity);
            esTemplate.refresh(esIndex);
        }
        else {
            this.update(esIndex, esType, esEntity.get(0), entity);
            esTemplate.refresh(esIndex);
        }
    }

    public void save(final String indexName, final String typeName, final Entity entity) {

        final Gson gson = new Gson();

        final ElasticSearchEntity esEntity = new ElasticSearchEntity(entity.getEntityId(), entity.getEntityName(),
                entity.getEntityDescription(), entity.getIsActiveYN(), entity.getCreatedUser(),
                entity.getCreatedTimestamp(), entity.getUpdatedUser(), entity.getUpdatedTimestamp());

        final IndexResponse response = this.client.prepareIndex(indexName, typeName, esEntity.get_id())
                .setSource(gson.toJson(esEntity), XContentType.JSON)
                .get();

    }

    public String update(final String indexName, final String typeName,
            final ElasticSearchEntity esEntity, final Entity entity)
            throws IOException, InterruptedException, ExecutionException {

        final Gson gson = new Gson();

        final UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index(indexName).type(typeName)
                .id(esEntity.get_id())
                .doc(gson.toJson(entity), XContentType.JSON);

        final UpdateResponse updateResponse = this.client.update(updateRequest).get();
        return updateResponse.status().toString();
    }

}
