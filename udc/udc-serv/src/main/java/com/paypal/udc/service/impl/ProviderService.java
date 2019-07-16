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
import com.paypal.udc.dao.integration.common.SourceProviderRepository;
import com.paypal.udc.entity.integration.common.SourceProvider;
import com.paypal.udc.exception.ValidationError;
import com.paypal.udc.service.IProviderService;
import com.paypal.udc.util.ProviderUtil;
import com.paypal.udc.util.UserUtil;
import com.paypal.udc.validator.provider.SourceProviderDescValidator;
import com.paypal.udc.validator.provider.SourceProviderNameValidator;


@Service
@Transactional(propagation = Propagation.SUPPORTS, readOnly = true)
public class ProviderService implements IProviderService {

    final static Logger logger = LoggerFactory.getLogger(ProviderService.class);
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss");

    @Autowired
    private SourceProviderRepository providerRepository;
    @Autowired
    private UserUtil userUtil;
    @Autowired
    private ProviderUtil providerUtil;
    @Autowired
    private SourceProviderDescValidator s2;
    @Autowired
    private SourceProviderNameValidator s1;

    @Value("${elasticsearch.sourceprovider.name}")
    private String esProviderIndex;

    @Value("${elasticsearch.type.name}")
    private String esType;

    @Value("${udc.es.write.enabled}")
    private String isEsWriteEnabled;

    @Autowired
    private ElasticsearchTemplate esTemplate;

    @Override
    public List<SourceProvider> getAllProviders() {
        final List<SourceProvider> sourceProviders = new ArrayList<SourceProvider>();
        this.providerRepository.findAll().forEach(sourceProviders::add);
        return sourceProviders;
    }

    @Override
    public SourceProvider getProviderById(final long providerId) throws ValidationError {
        final SourceProvider provider = this.providerUtil.validateProvider(providerId);
        return provider;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED, readOnly = false, rollbackFor = { ValidationError.class,
            ConstraintViolationException.class, DataIntegrityViolationException.class,
            TransactionSystemException.class, IOException.class, InterruptedException.class, ExecutionException.class })
    public SourceProvider addProvider(final SourceProvider provider)
            throws ValidationError, IOException, InterruptedException, ExecutionException {
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final ValidationError v = new ValidationError();
        SourceProvider insertedProvider = new SourceProvider();

        try {
            // final String createdUser = provider.getCreatedUser();
            // this.userUtil.validateUser(createdUser);
            provider.setUpdatedUser(provider.getCreatedUser());
            provider.setCreatedTimestamp(sdf.format(timestamp));
            provider.setUpdatedTimestamp(sdf.format(timestamp));
            insertedProvider = this.providerRepository.save(provider);
        }
        catch (final ConstraintViolationException e) {
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("Source Provider name is empty");
            throw v;
        }
        catch (final DataIntegrityViolationException e) {
            v.setErrorCode(HttpStatus.CONFLICT);
            v.setErrorDescription("Source Provider name is duplicated");
            throw v;
        }

        if (this.isEsWriteEnabled.equals("true")) {
            this.providerUtil.upsertProviders(this.esProviderIndex, this.esType, provider, this.esTemplate);
        }
        return insertedProvider;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED, readOnly = false, rollbackFor = { ValidationError.class,
            ConstraintViolationException.class, DataIntegrityViolationException.class,
            TransactionSystemException.class, IOException.class, InterruptedException.class, ExecutionException.class })
    public SourceProvider updateProvider(final SourceProvider provider)
            throws ValidationError, IOException, InterruptedException, ExecutionException {
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final ValidationError v = new ValidationError();
        SourceProvider tempProvider = this.providerUtil.validateProvider(provider.getSourceProviderId());
        try {
            // this.userUtil.validateUser(provider.getUpdatedUser());
            tempProvider.setUpdatedUser(provider.getUpdatedUser());
            tempProvider.setUpdatedTimestamp(sdf.format(timestamp));
            this.s1.setNextChain(this.s2);
            this.s1.validate(provider, tempProvider);
            tempProvider = this.providerRepository.save(tempProvider);
        }
        catch (final TransactionSystemException e) {
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("Provider name is empty");
            throw v;
        }
        catch (final DataIntegrityViolationException e) {
            v.setErrorCode(HttpStatus.CONFLICT);
            v.setErrorDescription(" Provider name is duplicated");
            throw v;
        }
        if (this.isEsWriteEnabled.equals("true")) {
            this.providerUtil.upsertProviders(this.esProviderIndex, this.esType, provider, this.esTemplate);
        }
        return tempProvider;
    }

    @Override
    public SourceProvider getProviderByName(final String providerName) {
        return this.providerRepository.findBySourceProviderName(providerName);
    }

}
