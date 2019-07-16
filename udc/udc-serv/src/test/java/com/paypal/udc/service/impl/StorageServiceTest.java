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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.util.ReflectionTestUtils;
import com.paypal.udc.dao.storagecategory.StorageRepository;
import com.paypal.udc.entity.storagecategory.Storage;
import com.paypal.udc.util.StorageUtil;
import com.paypal.udc.util.UserUtil;
import com.paypal.udc.util.enumeration.ActiveEnumeration;
import com.paypal.udc.validator.storage.StorageDescValidator;
import com.paypal.udc.validator.storage.StorageNameValidator;


@RunWith(SpringRunner.class)
public class StorageServiceTest {

    @Value("${udc.es.write.enabled}")
    String isEsWriteEnabled;

    @MockBean
    private UserUtil userUtil;

    @MockBean
    private StorageUtil storageUtil;

    @Mock
    private StorageRepository storageRepository;

    @Mock
    private StorageDescValidator s2;

    @Mock
    private StorageNameValidator s1;

    @InjectMocks
    private StorageService storageService;

    private Storage storage;
    private Long storageId;
    private String storageName;
    private List<Storage> storageList;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        this.storageId = 1L;
        this.storageName = "storageName";
        this.storage = new Storage(this.storageId, this.storageName, "storageDescription", "crUser", "crTime",
                "updUser", "updTime");
        this.storageList = Arrays.asList(this.storage);
    }

    @Test
    public void verifyValidGetAllStorages() throws Exception {
        when(this.storageRepository.findAll()).thenReturn(this.storageList);

        final List<Storage> result = this.storageService.getAllStorages();
        assertEquals(this.storageList.size(), result.size());

        verify(this.storageRepository).findAll();
    }

    @Test
    public void verifyValidGetStorageById() throws Exception {
        when(this.storageUtil.validateStorageId(this.storageId)).thenReturn(this.storage);

        final Storage result = this.storageService.getStorageById(this.storageId);
        assertEquals(this.storage, result);

        verify(this.storageUtil).validateStorageId(this.storageId);
    }

    @Test
    public void verifyValidGetStorageByName() throws Exception {
        when(this.storageRepository.findByStorageName(this.storageName)).thenReturn(this.storage);

        final Storage result = this.storageService.getStorageByName(this.storageName);
        assertEquals(this.storage, result);

        verify(this.storageRepository).findByStorageName(this.storageName);
    }

    @Test
    public void verifyValidAddStorage() throws Exception {
        ReflectionTestUtils.setField(this.storageService, "isEsWriteEnabled", "true");
        when(this.storageRepository.save(this.storage)).thenReturn(this.storage);
        final Storage result = this.storageService.addStorage(this.storage);
        assertEquals(this.storage, result);

        verify(this.storageRepository).save(this.storage);
    }

    @Test
    public void verifyValidUpdateStorage() throws Exception {
        ReflectionTestUtils.setField(this.storageService, "isEsWriteEnabled", "true");
        when(this.storageUtil.validateStorageId(this.storageId)).thenReturn(this.storage);
        when(this.storageRepository.save(this.storage)).thenReturn(this.storage);

        final Storage result = this.storageService.updateStorage(this.storage);
        assertEquals(this.storage, result);

        verify(this.storageRepository).save(this.storage);
    }

    @Test
    public void verifyValidDeleteStorage() throws Exception {
        ReflectionTestUtils.setField(this.storageService, "isEsWriteEnabled", "true");
        when(this.storageUtil.validateStorageId(this.storageId)).thenReturn(this.storage);
        when(this.storageRepository.save(this.storage)).thenReturn(this.storage);

        final Storage result = this.storageService.deleteStorage(this.storageId);
        assertEquals(this.storage, result);
        assertEquals(ActiveEnumeration.NO.getFlag(), result.getIsActiveYN());

        verify(this.storageRepository).save(this.storage);
    }

    @Test
    public void verifyValidEnableStorage() throws Exception {
        ReflectionTestUtils.setField(this.storageService, "isEsWriteEnabled", "true");
        when(this.storageUtil.validateStorageId(this.storageId)).thenReturn(this.storage);
        when(this.storageRepository.save(this.storage)).thenReturn(this.storage);

        final Storage result = this.storageService.enableStorage(this.storageId);
        assertEquals(this.storage, result);
        assertEquals(ActiveEnumeration.YES.getFlag(), result.getIsActiveYN());

        verify(this.storageRepository).save(this.storage);
    }
}
