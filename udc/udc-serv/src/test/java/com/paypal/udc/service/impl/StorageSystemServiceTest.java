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
import org.springframework.test.util.ReflectionTestUtils;
import com.paypal.udc.dao.cluster.ClusterRepository;
import com.paypal.udc.dao.storagesystem.StorageSystemAttributeValueRepository;
import com.paypal.udc.dao.storagesystem.StorageSystemContainerRepository;
import com.paypal.udc.dao.storagesystem.StorageSystemRepository;
import com.paypal.udc.dao.storagetype.StorageTypeAttributeKeyRepository;
import com.paypal.udc.dao.storagetype.StorageTypeRepository;
import com.paypal.udc.entity.entity.Entity;
import com.paypal.udc.entity.storagesystem.StorageSystem;
import com.paypal.udc.entity.storagesystem.StorageSystemAttributeValue;
import com.paypal.udc.entity.storagesystem.StorageSystemContainer;
import com.paypal.udc.entity.storagetype.StorageType;
import com.paypal.udc.entity.zone.Zone;
import com.paypal.udc.util.ClusterUtil;
import com.paypal.udc.util.EntityUtil;
import com.paypal.udc.util.StorageSystemUtil;
import com.paypal.udc.util.StorageTypeUtil;
import com.paypal.udc.util.UserUtil;
import com.paypal.udc.util.ZoneUtil;
import com.paypal.udc.util.enumeration.ActiveEnumeration;
import com.paypal.udc.util.enumeration.RunFrequencyEnumeration;
import com.paypal.udc.validator.storagesystem.StorageSystemDescValidator;
import com.paypal.udc.validator.storagesystem.StorageSystemNameValidator;
import com.paypal.udc.validator.storagesystem.StorageSystemTypeIDValidator;


@RunWith(SpringRunner.class)
public class StorageSystemServiceTest {

    @Mock
    private StorageSystemRepository storageSystemRepository;

    @Mock
    private StorageSystemContainerRepository storageSystemContainerRepository;

    @Mock
    private ClusterRepository clusterRepository;

    @Mock
    private ClusterUtil clusterUtil;

    @Mock
    private StorageTypeRepository storageTypeRepository;

    @Mock
    private StorageSystemAttributeValueRepository systemAttributeValueRepository;

    @Mock
    private StorageTypeAttributeKeyRepository typeAttributeKeyRepository;

    @Mock
    private UserUtil userUtil;

    @Mock
    private ZoneUtil zoneUtil;

    @Mock
    private EntityUtil entityUtil;

    @Mock
    private StorageSystemContainerRepository systemContainerRepository;

    @Mock
    private StorageTypeUtil storageTypeUtil;

    @Mock
    private StorageSystemUtil storageSystemUtil;

    @Mock
    private StorageSystemTypeIDValidator s3;

    @Mock
    private StorageSystemNameValidator s1;

    @Mock
    private StorageSystemDescValidator s2;

    @Mock
    private StorageSystemAttributeValueRepository ssavr;

    @InjectMocks
    private StorageSystemService storageSystemService;

    @Mock
    private StorageSystem insStorageSystem;

    private Long storageSystemId;
    private Long clusterId;
    private Long zoneId;
    private Long entityId;
    private String storageSystemName;
    private Long storageTypeId;
    private StorageSystem storageSystem;
    private List<StorageSystem> storageSystemsList;
    private StorageSystemAttributeValue attributeValue;
    private List<StorageSystemAttributeValue> attributeValuesList;
    private StorageType storageType;
    private Map<Long, StorageType> storageTypes;
    private Zone zone;
    private Entity entity;
    private Map<Long, Zone> zones;
    private Map<Long, Entity> entities;
    private StorageSystemContainer ssc;
    private List<StorageSystemContainer> sscs;
    private String storageTypeName;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        this.storageSystemId = 0L;
        this.storageSystemName = "storageSystemName";
        this.storageTypeId = 0L;
        this.clusterId = 0L;
        this.zoneId = 0L;
        this.entityId = 0L;
        this.storageSystem = new StorageSystem(this.storageSystemId, this.storageSystemName, "storageSystemDescription",
                "crUser", "crTime", "updUser", "updTime", this.storageTypeId, this.clusterId, this.zoneId,
                this.entityId, "Y",
                RunFrequencyEnumeration.SIXTY.getFlag());
        this.storageSystem.setIsActiveYN(ActiveEnumeration.YES.getFlag());
        this.storageSystemsList = Arrays.asList(this.storageSystem);
        this.attributeValue = new StorageSystemAttributeValue();
        this.attributeValue.setStorageDataSetAttributeKeyId(4L);
        this.attributeValuesList = Arrays.asList(this.attributeValue);
        this.storageType = new StorageType("storageTypeName", "storageTypeDescription", "crUser", "crTime", "updUser",
                "updTime", 0L);
        this.zone = new Zone("zoneName", "zoneDescription", "isActiveYN", "createdUser", "createdTimestamp",
                "updatedUser", "updatedTimestamp");
        this.entity = new Entity("entityName", "entityDescription", "isActiveYN", "createdUser", "createdTimestamp",
                "updatedUser", "updatedTimestamp");
        this.ssc = new StorageSystemContainer();
        this.sscs = Arrays.asList(this.ssc);
        this.storageTypeName = "All";
        this.zones = new HashMap<Long, Zone>();
        this.zones.put(0L, this.zone);
        this.entities = new HashMap<Long, Entity>();
        this.entities.put(0L, this.entity);
    }

    @Test
    public void verifyValidGetStorageSystemByStorageType() throws Exception {
        when(this.storageSystemRepository.findByStorageTypeId(this.storageTypeId)).thenReturn(this.storageSystemsList);

        final List<StorageSystem> result = this.storageSystemService
                .getStorageSystemByStorageType(this.storageSystemId);
        assertEquals(this.storageSystemsList, result);

        verify(this.storageSystemRepository).findByStorageTypeId(this.storageTypeId);
    }

    @Test
    public void verifyValidGetStorageSystemAttributes() throws Exception {
        when(this.storageSystemUtil.getAttributes(this.storageSystemId)).thenReturn(this.attributeValuesList);

        final List<StorageSystemAttributeValue> result = this.storageSystemService
                .getStorageSystemAttributes(this.storageSystemId);
        assertEquals(this.attributeValuesList, result);

        verify(this.storageSystemUtil).getAttributes(this.storageSystemId);
    }

    @Test
    public void verifyValidDeleteStorageSystem() throws Exception {
        ReflectionTestUtils.setField(this.storageSystemService, "isEsWriteEnabled", "true");
        when(this.storageSystemUtil.validateStorageSystem(this.storageSystemId)).thenReturn((this.storageSystem));
        when(this.ssavr.findByStorageSystemIdAndIsActiveYN(this.storageSystemId, ActiveEnumeration.YES.getFlag()))
                .thenReturn(this.attributeValuesList);
        when(this.storageSystemRepository.save(this.storageSystem)).thenReturn(this.storageSystem);
        when(this.ssavr.saveAll(this.attributeValuesList)).thenReturn(this.attributeValuesList);
        when(this.storageSystemContainerRepository.findByStorageSystemId(this.storageSystemId)).thenReturn(this.sscs);

        final StorageSystem result = this.storageSystemService.deleteStorageSystem(this.storageSystemId);
        assertEquals(this.storageSystem, result);

        verify(this.storageSystemUtil).validateStorageSystem(this.storageSystemId);
    }

    @Test
    public void verifyValidGetStorageSystemByType() throws Exception {
        when(this.storageSystemRepository.findAll()).thenReturn(this.storageSystemsList);
        when(this.zoneUtil.getZones()).thenReturn(this.zones);
        when(this.entityUtil.getEntities()).thenReturn(this.entities);
        final List<StorageSystem> result = this.storageSystemService.getStorageSystemByType(this.storageTypeName);
        assertEquals(this.storageSystemsList, result);

        verify(this.storageSystemRepository).findAll();
    }

    public void verifyValidEnableStorageSystem() throws Exception {
        when(this.storageSystemUtil.validateStorageSystem(this.storageSystemId)).thenReturn((this.storageSystem));
        when(this.storageSystemRepository.save(this.storageSystem)).thenReturn(this.storageSystem);
        when(this.ssavr.saveAll(this.attributeValuesList)).thenReturn(this.attributeValuesList);
        when(this.ssavr.findByStorageSystemIdAndIsActiveYN(this.storageSystemId, ActiveEnumeration.NO.getFlag()))
                .thenReturn(this.attributeValuesList);
        when(this.ssavr.saveAll(this.attributeValuesList)).thenReturn(this.attributeValuesList);

        final StorageSystem result = this.storageSystemService.enableStorageSystem(this.storageSystemId);
        assertEquals(this.storageSystem, result);

        verify(this.storageSystemUtil).validateStorageSystem(this.storageSystemId);
    }
}
