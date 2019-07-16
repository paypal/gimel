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

package com.paypal.udc.service.impl;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import javax.validation.ConstraintViolationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import com.paypal.udc.dao.zone.ZoneRepository;
import com.paypal.udc.entity.zone.Zone;
import com.paypal.udc.exception.ValidationError;
import com.paypal.udc.service.IZoneService;
import com.paypal.udc.util.ZoneUtil;
import com.paypal.udc.util.enumeration.ActiveEnumeration;
import com.paypal.udc.validator.zone.ZoneDescValidator;
import com.paypal.udc.validator.zone.ZoneNameValidator;


@Service
@Transactional(propagation = Propagation.SUPPORTS, readOnly = true)
public class ZoneService implements IZoneService {

    @Autowired
    private ZoneRepository zoneRepository;
    @Autowired
    private ZoneUtil zoneUtil;
    @Autowired
    private ZoneNameValidator s1;
    @Autowired
    private ZoneDescValidator s2;

    @Value("${elasticsearch.zone.name}")
    private String esZoneIndex;

    @Value("${elasticsearch.type.name}")
    private String esType;

    @Value("${udc.es.write.enabled}")
    private String isEsWriteEnabled;

    @Autowired
    private ElasticsearchTemplate esTemplate;

    final static Logger logger = LoggerFactory.getLogger(ClusterService.class);
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss");

    @Override
    public List<Zone> getAllZones() {
        final List<Zone> zones = new ArrayList<Zone>();
        this.zoneRepository.findAll().forEach(zone -> zones.add(zone));
        return zones;
    }

    @Override
    public Zone getZoneById(final long zoneId) throws ValidationError {
        final Zone zone = this.zoneUtil.validateZone(zoneId);
        return zone;
    }

    @Override
    public Zone addZone(final Zone zone) throws ValidationError, IOException, InterruptedException, ExecutionException {
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final ValidationError v = new ValidationError();
        Zone insertedZone = new Zone();
        try {
            // final String createdUser = zone.getCreatedUser();
            // this.userUtil.validateUser(createdUser);
            zone.setUpdatedUser(zone.getCreatedUser());
            zone.setCreatedTimestamp(sdf.format(timestamp));
            zone.setUpdatedTimestamp(sdf.format(timestamp));
            zone.setIsActiveYN(ActiveEnumeration.YES.getFlag());
            insertedZone = this.zoneRepository.save(zone);
        }
        catch (final ConstraintViolationException e) {
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("Zone name is empty");
            throw v;
        }
        catch (final DataIntegrityViolationException e) {
            v.setErrorCode(HttpStatus.CONFLICT);
            v.setErrorDescription("Zone name is duplicated");
            throw v;
        }

        if (this.isEsWriteEnabled.equals("true")) {
            this.zoneUtil.upsertZones(this.esZoneIndex, this.esType, zone, this.esTemplate);
        }
        return insertedZone;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED, readOnly = false, rollbackFor = { ValidationError.class,
            ConstraintViolationException.class, DataIntegrityViolationException.class,
            TransactionSystemException.class, IOException.class, InterruptedException.class, ExecutionException.class })
    public Zone updateZone(final Zone zone)
            throws ValidationError, IOException, InterruptedException, ExecutionException {
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final ValidationError v = new ValidationError();
        Zone tempZone = this.zoneUtil.validateZone(zone.getZoneId());
        try {
            tempZone.setUpdatedUser(zone.getUpdatedUser());
            tempZone.setUpdatedTimestamp(sdf.format(timestamp));
            this.s1.setNextChain(this.s2);
            this.s1.validate(zone, tempZone);
            tempZone = this.zoneRepository.save(tempZone);
        }
        catch (final TransactionSystemException e) {
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("Zone name is empty");
            throw v;
        }
        catch (final DataIntegrityViolationException e) {
            v.setErrorCode(HttpStatus.CONFLICT);
            v.setErrorDescription("Zone name is duplicated");
            throw v;
        }
        if (this.isEsWriteEnabled.equals("true")) {
            this.zoneUtil.upsertZones(this.esZoneIndex, this.esType, tempZone, this.esTemplate);
        }
        return tempZone;
    }

    @Override
    public Zone deActivateZone(final long zoneId)
            throws ValidationError, IOException, InterruptedException, ExecutionException {
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        Zone tempZone = this.zoneUtil.validateZone(zoneId);
        tempZone.setUpdatedTimestamp(sdf.format(timestamp));
        tempZone.setIsActiveYN(ActiveEnumeration.NO.getFlag());
        tempZone = this.zoneRepository.save(tempZone);
        if (this.isEsWriteEnabled.equals("true")) {
            this.zoneUtil.upsertZones(this.esZoneIndex, this.esType, tempZone, this.esTemplate);
        }
        return tempZone;
    }

    @Override
    public Zone getZoneByName(final String zoneName) {
        return this.zoneRepository.findByZoneName(zoneName);
    }

    @Override
    public Zone reActivateZone(final long zoneId)
            throws ValidationError, IOException, InterruptedException, ExecutionException {
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        Zone tempZone = this.zoneUtil.validateZone(zoneId);
        tempZone.setUpdatedTimestamp(sdf.format(timestamp));
        tempZone.setIsActiveYN(ActiveEnumeration.YES.getFlag());
        tempZone = this.zoneRepository.save(tempZone);
        if (this.isEsWriteEnabled.equals("true")) {
            this.zoneUtil.upsertZones(this.esZoneIndex, this.esType, tempZone, this.esTemplate);
        }
        return tempZone;
    }
}
