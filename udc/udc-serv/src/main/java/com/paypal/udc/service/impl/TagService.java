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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import javax.validation.ConstraintViolationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import com.paypal.udc.dao.integration.common.TagRepository;
import com.paypal.udc.entity.integration.common.DatasetTagMapInput;
import com.paypal.udc.entity.integration.common.Tag;
import com.paypal.udc.exception.ValidationError;
import com.paypal.udc.service.ITagService;
import com.paypal.udc.util.TagUtil;


@Service
@Transactional(propagation = Propagation.SUPPORTS, readOnly = true)
public class TagService implements ITagService {
    @Autowired
    private TagRepository tagRepository;

    @Autowired
    private TagUtil tagUtil;

    final static Logger logger = LoggerFactory.getLogger(TagService.class);
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss");

    @Override
    public List<Tag> getAllTags() {
        final List<Tag> tags = new ArrayList<Tag>();
        this.tagRepository.findAll().forEach(tags::add);
        return tags;
    }

    @Override
    public Tag getTagById(final long tagId) throws ValidationError {
        final Tag tag = this.tagUtil.validateTag(tagId);
        return tag;
    }

    @Override
    public Tag addTag(final Tag tag) throws ValidationError {
        return this.tagUtil.postTag(tag, sdf);
    }

    @Override
    public Tag updateTag(final Tag tag) throws ValidationError {
        return this.tagUtil.updateTag(tag, sdf);
    }

    @Override
    public Tag getTagByName(final String tagName) {
        return this.tagRepository.findByTagName(tagName);
    }

    @Override
    public List<Tag> getByProviderId(final long providerId) {
        return this.tagRepository.findByProviderId(providerId);
    }

    @Override
    public List<Tag> getByTagIds(final String ids) {
        final List<Long> longList = new ArrayList<Long>();
        final List<String> idList = Arrays.asList(ids.split(","));
        for (final String s : idList) {
            longList.add(Long.valueOf(s));
        }
        return this.tagRepository.findByTagIdIn(longList);
    }

    @Override
    public List<Tag> getTagsByDatasetId(final long datasetId) {
        return this.tagUtil.getTagsByDatasetId(datasetId);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED, readOnly = false, rollbackFor = { ValidationError.class,
            ConstraintViolationException.class, DataIntegrityViolationException.class, IOException.class,
            InterruptedException.class, ExecutionException.class })
    public void addTagForDataset(final DatasetTagMapInput datasetTagMapInput)
            throws ValidationError, IOException, InterruptedException, ExecutionException {
        this.tagUtil.addDatasetTagMap(datasetTagMapInput, sdf);
    }
}
