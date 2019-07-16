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
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import javax.validation.ConstraintViolationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.transaction.TransactionSystemException;
import com.paypal.udc.dao.dataset.DatasetChangeLogRepository;
import com.paypal.udc.dao.dataset.DatasetRepository;
import com.paypal.udc.dao.integration.common.DatasetTagMapRepository;
import com.paypal.udc.dao.integration.common.TagRepository;
import com.paypal.udc.entity.dataset.Dataset;
import com.paypal.udc.entity.dataset.DatasetChangeLog;
import com.paypal.udc.entity.integration.common.DatasetTagMap;
import com.paypal.udc.entity.integration.common.DatasetTagMapInput;
import com.paypal.udc.entity.integration.common.Tag;
import com.paypal.udc.exception.ValidationError;
import com.paypal.udc.util.enumeration.TimelineEnumeration;


@Component
public class TagUtil {

    @Autowired
    private TagRepository tagRepository;

    @Autowired
    private ProviderUtil providerUtil;

    @Autowired
    DatasetTagMapRepository datasetTagRepository;

    @Autowired
    private DatasetRepository datasetRepository;

    @Autowired
    private DatasetChangeLogRepository changeLogRepository;

    @Value("${elasticsearch.dataset.name}")
    private String esDatasetIndex;

    @Value("${elasticsearch.type.name}")
    private String esType;

    @Value("${udc.es.write.enabled}")
    private String isEsWriteEnabled;

    @Autowired
    private ElasticsearchTemplate esTemplate;

    @Autowired
    private DatasetUtil datasetUtil;

    public Tag validateTag(final long tagId) throws ValidationError {
        final Tag tag = this.tagRepository.findById(tagId).orElse(null);
        if (tag == null) {
            final ValidationError v = new ValidationError();
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("TagID is incorrect");
            throw v;
        }
        return tag;
    }

    public void addDatasetTagMap(final DatasetTagMapInput datasetTagMapInput, final SimpleDateFormat sdf)
            throws ValidationError, IOException, InterruptedException, ExecutionException {
        final String tagName = datasetTagMapInput.getTagName();
        final String createdUser = datasetTagMapInput.getCreatedUser();
        final String providerName = datasetTagMapInput.getProviderName();
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final String time = sdf.format(timestamp);
        String prevTagList = "{}";
        final long providerId = this.providerUtil.getProviderByName(providerName);

        Tag tag = this.tagRepository.findByTagNameAndProviderIdAndCreatedUser(tagName, providerId, createdUser);
        if (tag == null) {
            // post the tag
            tag = new Tag(tagName, providerId, createdUser, time, createdUser, time);
            tag = this.postTag(tag, sdf);
        }
        // post to pc_dataset_tag_map table
        try {
            DatasetTagMap datasetTagMap = new DatasetTagMap(datasetTagMapInput.getDatasetId(), tag.getTagId(),
                    createdUser, time, createdUser, time);

            final Dataset datasetTag = this.datasetRepository.findById(datasetTagMapInput.getDatasetId()).orElse(null);

            if (datasetTag != null) {
                final List<Tag> tags = this.getTagsByDatasetId(datasetTag.getStorageDataSetId());

                final List<String> tagsList = new ArrayList<>();

                if (!tags.isEmpty()) {
                    for (int i = 0; i < tags.size(); i++) {
                        final String prev = "{\"value\": \"" + tags.get(i).getTagName() + "\", \"username\": \""
                                + tags.get(i).getUpdatedUser()
                                + "\"}";
                        tagsList.add(prev);
                    }
                    prevTagList = tagsList.toString();
                }

                datasetTagMap = this.datasetTagRepository.save(datasetTagMap);

                final List<String> currTagsList = new ArrayList<>();

                // insert into changeLog table on creation of new tags
                final String curr = "{\"value\": \"" + tag.getTagName() + "\", \"username\": \""
                        + tag.getUpdatedUser()
                        + "\"}";
                currTagsList.addAll(tagsList);
                currTagsList.add(curr);

                final DatasetChangeLog dcl = new DatasetChangeLog(datasetTagMapInput.getDatasetId(), "C",
                        TimelineEnumeration.TAG.getFlag(),
                        prevTagList,
                        currTagsList.toString(),
                        time);
                this.changeLogRepository.save(dcl);
            }
        }
        catch (final DataIntegrityViolationException e) {
            final ValidationError v = new ValidationError();
            v.setErrorCode(HttpStatus.CONFLICT);
            v.setErrorDescription("Duplicated tag_id and dataset_id");
            throw v;
        }

        if (this.isEsWriteEnabled.equals("true")) {
            final Dataset tempDataset = this.datasetRepository.findById(datasetTagMapInput.getDatasetId())
                    .orElse(null);
            if (tempDataset != null) {
                this.datasetUtil.upsertDataset(this.esDatasetIndex, this.esType, tempDataset, this.esTemplate);
            }
        }

    }

    public List<Tag> getTagsByDatasetId(final long storageDatasetId) {
        final List<DatasetTagMap> datasetTagMap = this.datasetTagRepository.findByStorageDatasetId(storageDatasetId);
        final List<Long> tagIds = datasetTagMap.stream().map(datasetTag -> datasetTag.getTagId())
                .collect(Collectors.toList());
        return this.tagRepository.findByTagIdIn(tagIds);
    }

    public Tag updateTag(final Tag tag, final SimpleDateFormat sdf) throws ValidationError {

        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final ValidationError v = new ValidationError();
        Tag tempTag = this.tagRepository.findById(tag.getTagId()).orElse(null);
        if (tempTag != null) {
            try {
                // this.userUtil.validateUser(tag.getUpdatedUser());
                this.providerUtil.validateProvider(tag.getProviderId());
                tempTag.setUpdatedUser(tag.getUpdatedUser());
                tempTag.setUpdatedTimestamp(sdf.format(timestamp));
                tempTag = this.tagRepository.save(tempTag);
            }
            catch (final TransactionSystemException e) {
                v.setErrorCode(HttpStatus.BAD_REQUEST);
                v.setErrorDescription("Tag name is empty");
                throw v;
            }
            catch (final DataIntegrityViolationException e) {
                v.setErrorCode(HttpStatus.CONFLICT);
                v.setErrorDescription("Tag name and Provider Id are duplicated");
                throw v;
            }
        }
        else {
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("Tag ID is invalid");
            throw v;
        }
        return tempTag;
    }

    public Tag postTag(final Tag tag, final SimpleDateFormat sdf) throws ValidationError {
        final Tag insertedTag;
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final ValidationError v = new ValidationError();
        try {
            final String createdUser = tag.getCreatedUser();
            // this.userUtil.validateUser(createdUser);
            this.providerUtil.validateProvider(tag.getProviderId());
            tag.setUpdatedUser(createdUser);
            tag.setCreatedTimestamp(sdf.format(timestamp));
            tag.setUpdatedTimestamp(sdf.format(timestamp));
            insertedTag = this.tagRepository.save(tag);
        }
        catch (final ConstraintViolationException e) {
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("Tag name is empty");
            throw v;
        }
        catch (final DataIntegrityViolationException e) {
            v.setErrorCode(HttpStatus.CONFLICT);
            v.setErrorDescription("Tag name and Provider Id pair is duplicated");
            throw v;
        }
        return insertedTag;
    }
}
