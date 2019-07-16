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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;
import com.paypal.udc.dao.storagecategory.StorageRepository;
import com.paypal.udc.dao.storagetype.StorageTypeAttributeKeyRepository;
import com.paypal.udc.dao.storagetype.StorageTypeRepository;
import com.paypal.udc.entity.storagecategory.Storage;
import com.paypal.udc.entity.storagetype.StorageType;
import com.paypal.udc.util.StorageTypeUtil;
import com.paypal.udc.util.enumeration.ActiveEnumeration;
import com.paypal.udc.validator.storagetype.StorageIDValidator;
import com.paypal.udc.validator.storagetype.StorageTypeDescValidator;
import com.paypal.udc.validator.storagetype.StorageTypeNameValidator;


@RunWith(SpringRunner.class)
public class StorageTypeServiceTest {

    @Mock
    private StorageTypeRepository storageTypeRepository;

    @Mock
    private StorageTypeAttributeKeyRepository storageAttributeRepository;

    @Mock
    private StorageTypeUtil storageTypeUtil;

    @Mock
    private StorageIDValidator s3;

    @Mock
    private StorageTypeNameValidator s1;

    @Mock
    private StorageTypeDescValidator s2;

    @Mock
    private StorageRepository storageRepository;

    @InjectMocks
    private StorageTypeService storageTypeService;

    @Mock
    private Map<Long, Storage> storageMap = new HashMap<Long, Storage>();
    private StorageType storageType;
    private List<StorageType> storageTypesList;
    private Storage storage;
    private Long storageTypeId;
    private Long storageId;
    private String storageName;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        this.storageType = new StorageType();
        this.storageType.setIsActiveYN(ActiveEnumeration.YES.getFlag());
        this.storageTypeId = 0L;
        this.storageId = 0L;
        this.storageType.setStorageId(this.storageId);
        this.storageTypesList = Arrays.asList(this.storageType);
        this.storage = new Storage();
        this.storageName = "All";
    }

    @Test
    public void verifyValidGetAllStorageTypes() throws Exception {
        when(this.storageTypeUtil.getStorages()).thenReturn(this.storageMap);
        when(this.storageTypeRepository.findAll()).thenReturn(this.storageTypesList);
        when(this.storageMap.get(this.storageTypeId)).thenReturn(this.storage);

        final List<StorageType> result = this.storageTypeService.getAllStorageTypes();
        assertEquals(this.storageTypesList.size(), result.size());

        verify(this.storageTypeRepository).findAll();
    }

    @Test
    public void verifyValidGetStorageTypeById() throws Exception {
        when(this.storageTypeUtil.getStorages()).thenReturn(this.storageMap);
        when(this.storageTypeUtil.validateStorageTypeId(this.storageId)).thenReturn(this.storageType);
        when(this.storageMap.get(0L)).thenReturn(this.storage);

        final StorageType result = this.storageTypeService.getStorageTypeById(this.storageTypeId);
        assertEquals(this.storageType, result);

        verify(this.storageTypeUtil).validateStorageTypeId(this.storageId);
    }

    @Test
    public void verifyValidGetStorageTypeByStorageCategory() throws Exception {
        when(this.storageTypeUtil.getStorages()).thenReturn(this.storageMap);
        when(this.storageTypeRepository.findByStorageId(this.storageId)).thenReturn(this.storageTypesList);
        when(this.storageMap.get(this.storageTypeId)).thenReturn(this.storage);

        final List<StorageType> result = this.storageTypeService.getStorageTypeByStorageCategory(this.storageId);
        assertEquals(this.storageTypesList.size(), result.size());

        verify(this.storageTypeRepository).findByStorageId(this.storageId);
    }

    @Test
    public void verifyValidGetStorageTypeByStorageCategoryName() throws Exception {
        when(this.storageTypeRepository.findAll()).thenReturn(this.storageTypesList);

        final List<StorageType> result = this.storageTypeService.getStorageTypeByStorageCategoryName(this.storageName);
        assertEquals(this.storageTypesList.size(), result.size());

        verify(this.storageTypeRepository).findAll();
    }

    @Test
    public void verifyValidEnableStorageType() throws Exception {
        when(this.storageTypeUtil.validateStorageTypeId(this.storageId)).thenReturn(this.storageType);
        when(this.storageTypeRepository.save(this.storageType)).thenReturn(this.storageType);

        final StorageType result = this.storageTypeService.enableStorageType(this.storageTypeId);
        assertEquals(this.storageType, result);

        verify(this.storageTypeRepository).save(this.storageType);
    }
}
