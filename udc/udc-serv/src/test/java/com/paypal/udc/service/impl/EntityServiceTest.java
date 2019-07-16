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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.Arrays;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.util.ReflectionTestUtils;
import com.paypal.udc.dao.entity.EntityRepository;
import com.paypal.udc.entity.entity.Entity;
import com.paypal.udc.util.EntityUtil;
import com.paypal.udc.util.UserUtil;
import com.paypal.udc.util.enumeration.ActiveEnumeration;
import com.paypal.udc.validator.entity.EntityDescValidator;
import com.paypal.udc.validator.entity.EntityNameValidator;


@RunWith(SpringRunner.class)
public class EntityServiceTest {

    @MockBean
    private UserUtil userUtil;

    @MockBean
    private EntityUtil entityUtil;

    @Mock
    private EntityRepository entityRepository;

    @Mock
    private EntityNameValidator s2;

    @Mock
    private EntityDescValidator s1;

    @InjectMocks
    private EntityService entityService;

    private long entityId;
    private String entityName;
    private Entity entity;
    private List<Entity> entityList;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        this.entityId = 0L;
        this.entityName = "entityName";
        this.entity = new Entity();
        this.entityList = Arrays.asList(this.entity);
    }

    @Test
    public void verifyValidGetEntityById() throws Exception {
        when(this.entityUtil.validateEntity(this.entityId)).thenReturn(this.entity);

        final Entity result = this.entityService.getEntityById(this.entityId);
        assertEquals(this.entity, result);

        verify(this.entityUtil).validateEntity(this.entityId);
    }

    @Test
    public void verifyValidGetEntityByName() throws Exception {
        when(this.entityRepository.findByEntityName(this.entityName)).thenReturn(this.entity);

        final Entity result = this.entityService.getEntityByName(this.entityName);
        assertEquals(this.entity, result);

        verify(this.entityRepository).findByEntityName(this.entityName);
    }

    @Test
    public void verifyValidGetAllentities() throws Exception {
        when(this.entityRepository.findAll()).thenReturn(this.entityList);

        final List<Entity> result = this.entityService.getAllEntities();
        assertEquals(this.entityList.size(), result.size());

        verify(this.entityRepository).findAll();
    }

    @Test
    public void verifyValidAddEntity() throws Exception {
        ReflectionTestUtils.setField(this.entityService, "isEsWriteEnabled", "true");
        when(this.entityRepository.save(this.entity)).thenReturn(this.entity);
        final Entity result = this.entityService.addEntity(this.entity);
        assertEquals(this.entity, result);
        assertEquals(ActiveEnumeration.YES.getFlag(), result.getIsActiveYN());

        verify(this.entityRepository).save(this.entity);
    }

    @Test
    public void verifyValidUpdateEntity() throws Exception {
        ReflectionTestUtils.setField(this.entityService, "isEsWriteEnabled", "true");
        when(this.entityUtil.validateEntity(this.entityId)).thenReturn(this.entity);
        when(this.entityRepository.save(this.entity)).thenReturn(this.entity);

        final Entity result = this.entityService.updateEntity(this.entity);
        assertEquals(this.entity, result);

        verify(this.entityRepository).save(this.entity);
    }

    @Test
    public void verifyValidDeActivateEntity() throws Exception {
        ReflectionTestUtils.setField(this.entityService, "isEsWriteEnabled", "true");
        when(this.entityUtil.validateEntity(this.entityId)).thenReturn(this.entity);
        when(this.entityRepository.save(this.entity)).thenReturn(this.entity);

        final Entity result = this.entityService.deActivateEntity(this.entityId);
        assertEquals(this.entity, result);
        assertEquals(ActiveEnumeration.NO.getFlag(), result.getIsActiveYN());

        verify(this.entityRepository).save(this.entity);
    }

    @Test
    public void verifyValidReActivateEntity() throws Exception {
        ReflectionTestUtils.setField(this.entityService, "isEsWriteEnabled", "true");
        when(this.entityUtil.validateEntity(this.entityId)).thenReturn(this.entity);
        when(this.entityRepository.save(this.entity)).thenReturn(this.entity);

        final Entity result = this.entityService.reActivateEntity(this.entityId);
        assertEquals(this.entity, result);
        assertEquals(ActiveEnumeration.YES.getFlag(), result.getIsActiveYN());

        verify(this.entityRepository).save(this.entity);
    }

}
