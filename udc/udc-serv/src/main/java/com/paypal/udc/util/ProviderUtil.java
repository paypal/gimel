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
import com.paypal.udc.dao.integration.common.ElasticSearchSourceProviderRepository;
import com.paypal.udc.dao.integration.common.SourceProviderRepository;
import com.paypal.udc.entity.integration.common.ElasticSearchSourceProvider;
import com.paypal.udc.entity.integration.common.SourceProvider;
import com.paypal.udc.exception.ValidationError;


@Component
public class ProviderUtil {

    @Autowired
    private SourceProviderRepository providerRepository;
    @Autowired
    private ElasticSearchSourceProviderRepository esSourceProviderRepository;
    @Autowired
    private TransportClient client;

    public SourceProvider validateProvider(final Long providerId) throws ValidationError {
        final SourceProvider provider = this.providerRepository.findById(providerId).orElse(null);
        if (provider == null) {
            final ValidationError v = new ValidationError();
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("Provider ID is incorrect");
            throw v;
        }
        return provider;
    }

    public long getProviderByName(final String providerName) throws ValidationError {
        // get source provider
        final SourceProvider provider = this.providerRepository.findBySourceProviderName(providerName);
        if (provider == null) {
            final ValidationError verror = new ValidationError();
            verror.setErrorCode(HttpStatus.CONFLICT);
            verror.setErrorDescription("Invalid Provider Name");
            throw verror;
        }
        final long providerId = provider.getSourceProviderId();
        return providerId;
    }

    public void upsertProviders(final String esProviderIndex, final String esType, final SourceProvider provider,
            final ElasticsearchTemplate esTemplate) throws IOException, InterruptedException, ExecutionException {
        final List<ElasticSearchSourceProvider> esProviders = this.esSourceProviderRepository
                .findBySourceProviderId(provider.getSourceProviderId());

        if (esProviders == null || esProviders.isEmpty()) {
            this.save(esProviderIndex, esType, provider);
            esTemplate.refresh(esProviderIndex);
        }
        else {
            this.update(esProviderIndex, esType, esProviders.get(0), provider);
            esTemplate.refresh(esProviderIndex);
        }
    }

    public void save(final String indexName, final String typeName, final SourceProvider sourceProvider) {
        final Gson gson = new Gson();

        final ElasticSearchSourceProvider esSourceProvider = new ElasticSearchSourceProvider(
                sourceProvider.getSourceProviderId(),
                sourceProvider.getSourceProviderName(),
                sourceProvider.getSourceProviderDescription(), sourceProvider.getCreatedUser(),
                sourceProvider.getCreatedTimestamp(),
                sourceProvider.getUpdatedUser(), sourceProvider.getUpdatedTimestamp());

        final IndexResponse response = this.client.prepareIndex(indexName, typeName, esSourceProvider.get_id())
                .setSource(gson.toJson(esSourceProvider), XContentType.JSON)
                .get();

    }

    public void update(final String indexName, final String typeName,
            final ElasticSearchSourceProvider esSourceProvider,
            final SourceProvider sourceProvider) throws IOException, InterruptedException, ExecutionException {
        final Gson gson = new Gson();

        final UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index(indexName).type(typeName)
                .id(esSourceProvider.get_id())
                .doc(gson.toJson(sourceProvider), XContentType.JSON);

        final UpdateResponse updateResponse = this.client.update(updateRequest).get();

    }

}
