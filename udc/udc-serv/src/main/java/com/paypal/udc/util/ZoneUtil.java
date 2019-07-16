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
import com.paypal.udc.dao.zone.ElasticSearchZoneRepository;
import com.paypal.udc.dao.zone.ZoneRepository;
import com.paypal.udc.entity.zone.ElasticSearchZone;
import com.paypal.udc.entity.zone.Zone;
import com.paypal.udc.exception.ValidationError;


@Component
public class ZoneUtil {

    @Autowired
    private ZoneRepository zoneRepository;
    @Autowired
    private ElasticSearchZoneRepository esZoneRepository;
    @Autowired
    private TransportClient client;

    public Map<Long, Zone> getZoneMappings() {
        final Map<Long, Zone> zoneMap = new HashMap<Long, Zone>();
        this.zoneRepository.findAll()
                .forEach(zone -> {
                    zoneMap.put(zone.getZoneId(), zone);
                });
        return zoneMap;
    }

    public Zone validateZone(final long zoneId) throws ValidationError {
        final Zone zone = this.zoneRepository.findById(zoneId).orElse(null);
        if (zone == null) {
            final ValidationError v = new ValidationError();
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("ZoneID is incorrect");
            throw v;
        }
        return zone;
    }

    public Map<Long, Zone> getZones() {
        final Map<Long, Zone> zones = new HashMap<Long, Zone>();
        this.zoneRepository.findAll()
                .forEach(zone -> {
                    zones.put(zone.getZoneId(), zone);
                });
        return zones;
    }

    public void save(final String indexName, final String typeName, final Zone zone) {

        final Gson gson = new Gson();

        final ElasticSearchZone esZone = new ElasticSearchZone(zone.getZoneId(), zone.getZoneName(),
                zone.getZoneDescription(), zone.getIsActiveYN(), zone.getCreatedUser(), zone.getCreatedTimestamp(),
                zone.getUpdatedUser(), zone.getUpdatedTimestamp());

        final IndexResponse response = this.client.prepareIndex(indexName, typeName, esZone.get_id())
                .setSource(gson.toJson(esZone), XContentType.JSON)
                .get();
    }

    public void upsertZones(final String esZoneIndex, final String esType, final Zone zone,
            final ElasticsearchTemplate esTemplate) throws IOException, InterruptedException, ExecutionException {
        final List<ElasticSearchZone> esZones = this.esZoneRepository.findByZoneId(zone.getZoneId());

        if (esZones == null || esZones.isEmpty()) {
            this.save(esZoneIndex, esType, zone);
            esTemplate.refresh(esZoneIndex);
        }
        else {
            this.update(esZoneIndex, esType, esZones.get(0), zone);
            esTemplate.refresh(esZoneIndex);
        }
    }

    public String update(final String indexName, final String typeName,
            final ElasticSearchZone esZone, final Zone zone)
            throws IOException, InterruptedException, ExecutionException {

        final Gson gson = new Gson();
        final UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index(indexName).type(typeName)
                .id(esZone.get_id())
                .doc(gson.toJson(zone), XContentType.JSON);

        final UpdateResponse updateResponse = this.client.update(updateRequest).get();
        return updateResponse.status().toString();
    }
}
