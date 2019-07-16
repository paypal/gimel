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
import com.paypal.udc.dao.entity.EntityRepository;
import com.paypal.udc.entity.entity.Entity;
import com.paypal.udc.exception.ValidationError;
import com.paypal.udc.service.IEntityService;
import com.paypal.udc.util.EntityUtil;
import com.paypal.udc.util.enumeration.ActiveEnumeration;
import com.paypal.udc.validator.entity.EntityDescValidator;
import com.paypal.udc.validator.entity.EntityNameValidator;


@Service
@Transactional(propagation = Propagation.SUPPORTS, readOnly = true)
public class EntityService implements IEntityService {

    @Autowired
    private EntityRepository entityRepository;
    @Autowired
    private EntityUtil entityUtil;
    @Autowired
    private EntityNameValidator s1;
    @Autowired
    private EntityDescValidator s2;

    @Value("${elasticsearch.entity.name}")
    private String esEntityIndex;

    @Value("${elasticsearch.type.name}")
    private String esType;

    @Value("${udc.es.write.enabled}")
    private String isEsWriteEnabled;

    @Autowired
    private ElasticsearchTemplate esTemplate;

    final static Logger logger = LoggerFactory.getLogger(ClusterService.class);
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss");

    @Override
    public List<Entity> getAllEntities() {
        final List<Entity> entities = new ArrayList<Entity>();
        this.entityRepository.findAll().forEach(entity -> entities.add(entity));
        return entities;
    }

    @Override
    public Entity getEntityById(final long entityId) throws ValidationError {
        final Entity entity = this.entityUtil.validateEntity(entityId);
        return entity;
    }

    @Override
    public Entity getEntityByName(final String entityName) {
        return this.entityRepository.findByEntityName(entityName);

    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED, readOnly = false, rollbackFor = { ValidationError.class,
            ConstraintViolationException.class, DataIntegrityViolationException.class,
            TransactionSystemException.class, IOException.class, InterruptedException.class, ExecutionException.class })
    public Entity addEntity(final Entity entity)
            throws ValidationError, IOException, InterruptedException, ExecutionException {
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final ValidationError v = new ValidationError();
        Entity insertedEntity = new Entity();
        try {
            // final String createdUser = entity.getCreatedUser();
            // this.userUtil.validateUser(createdUser);
            entity.setUpdatedUser(entity.getCreatedUser());
            entity.setCreatedTimestamp(sdf.format(timestamp));
            entity.setUpdatedTimestamp(sdf.format(timestamp));
            entity.setIsActiveYN(ActiveEnumeration.YES.getFlag());
            insertedEntity = this.entityRepository.save(entity);

        }
        catch (final ConstraintViolationException e) {
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("Entity name is empty");
            throw v;
        }
        catch (final DataIntegrityViolationException e) {
            v.setErrorCode(HttpStatus.CONFLICT);
            v.setErrorDescription("Entity name is duplicated");
            throw v;
        }

        if (this.isEsWriteEnabled.equals("true")) {
            this.entityUtil.upsertEntities(this.esEntityIndex, this.esType, entity, this.esTemplate);
        }
        return insertedEntity;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED, readOnly = false, rollbackFor = { ValidationError.class,
            ConstraintViolationException.class, DataIntegrityViolationException.class,
            TransactionSystemException.class, IOException.class, InterruptedException.class, ExecutionException.class })
    public Entity deActivateEntity(final long entityId)
            throws ValidationError, IOException, InterruptedException, ExecutionException {
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        Entity tempEntity = this.entityUtil.validateEntity(entityId);
        tempEntity.setUpdatedTimestamp(sdf.format(timestamp));
        tempEntity.setIsActiveYN(ActiveEnumeration.NO.getFlag());
        tempEntity = this.entityRepository.save(tempEntity);

        if (this.isEsWriteEnabled.equals("true")) {
            this.entityUtil.upsertEntities(this.esEntityIndex, this.esType, tempEntity, this.esTemplate);
        }

        return tempEntity;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED, readOnly = false, rollbackFor = { ValidationError.class,
            ConstraintViolationException.class, DataIntegrityViolationException.class,
            TransactionSystemException.class, IOException.class, InterruptedException.class, ExecutionException.class })
    public Entity reActivateEntity(final long entityId)
            throws ValidationError, IOException, InterruptedException, ExecutionException {
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        Entity tempEntity = this.entityUtil.validateEntity(entityId);

        tempEntity.setUpdatedTimestamp(sdf.format(timestamp));
        tempEntity.setIsActiveYN(ActiveEnumeration.YES.getFlag());
        tempEntity = this.entityRepository.save(tempEntity);
        if (this.isEsWriteEnabled.equals("true")) {
            this.entityUtil.upsertEntities(this.esEntityIndex, this.esType, tempEntity, this.esTemplate);
        }
        return tempEntity;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED, readOnly = false, rollbackFor = { ValidationError.class,
            ConstraintViolationException.class, DataIntegrityViolationException.class,
            TransactionSystemException.class, IOException.class, InterruptedException.class, ExecutionException.class })
    public Entity updateEntity(final Entity entity)
            throws ValidationError, IOException, InterruptedException, ExecutionException {
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final ValidationError v = new ValidationError();
        Entity tempEntity = this.entityUtil.validateEntity(entity.getEntityId());
        try {
            tempEntity.setUpdatedUser(entity.getUpdatedUser());
            tempEntity.setUpdatedTimestamp(sdf.format(timestamp));
            this.s1.setNextChain(this.s2);
            this.s1.validate(entity, tempEntity);
            tempEntity = this.entityRepository.save(tempEntity);

        }
        catch (final TransactionSystemException e) {
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("Entity name is empty");
            throw v;
        }
        catch (final DataIntegrityViolationException e) {
            v.setErrorCode(HttpStatus.CONFLICT);
            v.setErrorDescription("Entity name is duplicated");
            throw v;
        }
        if (this.isEsWriteEnabled.equals("true")) {
            this.entityUtil.upsertEntities(this.esEntityIndex, this.esType, tempEntity, this.esTemplate);
        }
        return tempEntity;
    }

}
