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
import com.paypal.udc.dao.integration.common.SourceProviderRepository;
import com.paypal.udc.entity.integration.common.SourceProvider;
import com.paypal.udc.util.ProviderUtil;
import com.paypal.udc.util.UserUtil;
import com.paypal.udc.validator.provider.SourceProviderDescValidator;
import com.paypal.udc.validator.provider.SourceProviderNameValidator;


@RunWith(SpringRunner.class)
public class ProviderServiceTest {

    @MockBean
    private UserUtil userUtil;

    @MockBean
    private ProviderUtil providerUtil;

    @Mock
    private SourceProviderRepository providerRepository;

    @Mock
    private SourceProviderDescValidator s2;

    @Mock
    private SourceProviderNameValidator s1;

    @InjectMocks
    private ProviderService providerService;

    private long providerId;
    private String providerName;
    private SourceProvider provider;
    private List<SourceProvider> providerList;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        this.providerId = 1L;
        this.providerName = "provider1";
        this.provider = new SourceProvider(this.providerName, "Description", "CrUser",
                "CrTime", "UpdUser", "UpdTime");
        this.provider.setSourceProviderId(this.providerId);
        this.providerList = Arrays.asList(this.provider);
    }

    @Test
    public void verifyValidGetAllProviders() throws Exception {
        when(this.providerRepository.findAll()).thenReturn(this.providerList);

        final List<SourceProvider> result = this.providerService.getAllProviders();
        assertEquals(this.providerList.size(), result.size());

        verify(this.providerRepository).findAll();
    }

    @Test
    public void verifyValidGetProviderById() throws Exception {
        when(this.providerUtil.validateProvider(this.providerId)).thenReturn((this.provider));

        final SourceProvider result = this.providerService.getProviderById(this.providerId);
        assertEquals(this.provider, result);

        verify(this.providerUtil).validateProvider(this.providerId);
    }

    @Test
    public void verifyValidGetProviderByName() throws Exception {
        when(this.providerRepository.findBySourceProviderName(this.providerName)).thenReturn(this.provider);

        final SourceProvider result = this.providerService.getProviderByName(this.providerName);
        assertEquals(this.provider, result);

        verify(this.providerRepository).findBySourceProviderName(this.providerName);
    }

    @Test
    public void verifyValidAddProvider() throws Exception {
        ReflectionTestUtils.setField(this.providerService, "isEsWriteEnabled", "true");
        when(this.providerRepository.save(this.provider)).thenReturn(this.provider);

        final SourceProvider result = this.providerService.addProvider(this.provider);
        assertEquals(this.provider, result);

        verify(this.providerRepository).save(this.provider);
    }

    @Test
    public void verifyValidUpdateProvider() throws Exception {
        ReflectionTestUtils.setField(this.providerService, "isEsWriteEnabled", "true");
        when(this.providerUtil.validateProvider(this.providerId)).thenReturn((this.provider));
        when(this.providerRepository.save(this.provider)).thenReturn(this.provider);

        final SourceProvider result = this.providerService.updateProvider(this.provider);
        assertEquals(this.provider, result);

        verify(this.providerRepository).save(this.provider);
    }

}
