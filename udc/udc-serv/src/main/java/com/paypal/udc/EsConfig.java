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

package com.paypal.udc;

import java.net.InetAddress;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.repository.config.EnableElasticsearchRepositories;


@Configuration
@EnableElasticsearchRepositories(basePackages = "com.paypal.udc.dao")
public class EsConfig {

    final static Logger logger = LoggerFactory.getLogger(EsConfig.class);

    @Value("${elasticsearch.host}")
    private String EsHost;

    @Value("${elasticsearch.port}")
    private int esPort;

    @Value("${elasticsearch.transport.port}")
    private int esTransportPort;

    @Value("${elasticsearch.sourceprovider.name}")
    private String providerIndexName;

    @Value("${elasticsearch.clustername}")
    private String EsClusterName;

    @Value("${elasticsearch.zone.name}")
    private String zoneIndexName;

    @Value("${elasticsearch.entity.name}")
    private String entityIndexName;

    @Value("${elasticsearch.category.index.name}")
    private String categoryIndexName;

    @Value("${elasticsearch.type.index.name}")
    private String typeIndexName;

    @Value("${elasticsearch.system.index.name}")
    private String systemIndexName;

    @Value("${elasticsearch.user.name}")
    private String userIndexName;

    @Value("${elasticsearch.dataset.name}")
    private String datasetIndexName;

    @Bean
    public String datasetIndexName() {
        return this.datasetIndexName;
    }

    @Bean
    public String zoneIndexName() {
        return this.zoneIndexName;
    }

    @Bean
    public String entityIndexName() {
        return this.entityIndexName;
    }

    @Bean
    public String categoryIndexName() {
        return this.categoryIndexName;
    }

    @Bean
    public String typeIndexName() {
        return this.typeIndexName;
    }

    @Bean
    public String systemIndexName() {
        return this.systemIndexName;
    }

    @Bean
    public String userIndexName() {
        return this.userIndexName;
    }

    @Bean
    public String providerIndexName() {
        return this.providerIndexName;
    }

    @Bean
    public Client client() throws Exception {

        final Settings esSettings = Settings.builder()
                .put("client.transport.ignore_cluster_name", true)
                .build();

        final TransportClient client = new PreBuiltTransportClient(esSettings);
        client.addTransportAddress(
                new TransportAddress(InetAddress.getByName(this.EsHost), this.esTransportPort));
        return client;
    }

    @Bean
    public ElasticsearchOperations elasticsearchTemplate() throws Exception {
        return new ElasticsearchTemplate(this.client());
    }

}
