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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;
import com.paypal.udc.dao.integration.common.DatasetTagMapRepository;
import com.paypal.udc.dao.integration.common.TagRepository;
import com.paypal.udc.entity.integration.common.DatasetTagMap;
import com.paypal.udc.entity.integration.common.Tag;
import com.paypal.udc.util.TagUtil;


@RunWith(SpringRunner.class)
public class TagServiceTest {

    @MockBean
    private TagUtil tagUtil;

    @Mock
    private TagRepository tagRepository;

    @InjectMocks
    private TagService tagService;

    @Mock
    DatasetTagMapRepository datasetTagRepository;

    private long datasetId;
    private long tagId, providerId;
    private String ids;
    private String tagName;
    private Tag tag;
    private List<Tag> tagList;
    private DatasetTagMap datasetTag;
    private List<DatasetTagMap> datasetTagMap;
    private List<Long> tagIds;
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss");

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        this.ids = "1";
        this.tagId = 1L;
        this.providerId = 1L;
        this.datasetId = 1L;
        this.tagName = "tag1";
        this.tag = new Tag(this.tagName, this.providerId, "CrUser",
                "CrTime", "UpdUser", "UpdTime");
        this.tag.setTagId(this.tagId);
        this.tagList = Arrays.asList(this.tag);

        this.tagIds = new ArrayList<Long>();
        this.tagIds.add(this.tagId);

        this.datasetTag = new DatasetTagMap(this.datasetId, this.tagId, "CrUser", "CrTime", "UdpUser", "UpdTime");
        this.datasetTagMap = new ArrayList<DatasetTagMap>();
        this.datasetTagMap.add(this.datasetTag);
    }

    @Test
    public void verifyValidGetTagsFromList() throws Exception {
        final List<Long> longList = new ArrayList<Long>();
        final List<String> tagIds = Arrays.asList(this.ids.split(","));
        for (final String s : tagIds) {
            longList.add(Long.valueOf(s));
        }

        when(this.tagRepository.findByTagIdIn(longList)).thenReturn(this.tagList);
        final List<Tag> result = this.tagService.getByTagIds(this.ids);
        assertEquals(this.tagList.size(), result.size());

        verify(this.tagRepository).findByTagIdIn(longList);
    }

    @Test
    public void verifyValidGetAllTags() throws Exception {
        when(this.tagRepository.findAll()).thenReturn(this.tagList);

        final List<Tag> result = this.tagService.getAllTags();
        assertEquals(this.tagList.size(), result.size());

        verify(this.tagRepository).findAll();
    }

    @Test
    public void verifyValidGetTagByProviderId() throws Exception {
        when(this.tagRepository.findByProviderId(this.providerId)).thenReturn(this.tagList);

        final List<Tag> result = this.tagService.getByProviderId(this.providerId);
        assertEquals(this.tagList, result);

        verify(this.tagRepository).findByProviderId(this.providerId);
    }

    @Test
    public void verifyValidGetTagById() throws Exception {
        when(this.tagUtil.validateTag(this.tagId)).thenReturn(this.tag);

        final Tag result = this.tagService.getTagById(this.tagId);
        assertEquals(this.tag, result);

        verify(this.tagUtil).validateTag(this.tagId);
    }

    @Test
    public void verifyValidGetTagByName() throws Exception {
        when(this.tagRepository.findByTagName(this.tagName)).thenReturn(this.tag);

        final Tag result = this.tagService.getTagByName(this.tagName);
        assertEquals(this.tag, result);

        verify(this.tagRepository).findByTagName(this.tagName);
    }

    @Test
    public void verifyValidAddTag() throws Exception {

        when(this.tagRepository.save(this.tag)).thenReturn(this.tag);
        when(this.tagUtil.postTag(this.tag, sdf)).thenReturn(this.tag);
        final Tag result = this.tagService.addTag(this.tag);
        assertEquals(this.tag, result);
    }

    @Test
    public void verifyValidUpdateTag() throws Exception {
        when(this.tagRepository.findById(this.tagId)).thenReturn(Optional.of(this.tag));
        when(this.tagRepository.save(this.tag)).thenReturn(this.tag);
        when(this.tagUtil.updateTag(this.tag, sdf)).thenReturn(this.tag);
        final Tag result = this.tagService.updateTag(this.tag);
        assertEquals(this.tag, result);
    }

    @Test
    public void verifyGetTagsByDatasetId() throws Exception {

        when(this.datasetTagRepository.findByStorageDatasetId(this.datasetId)).thenReturn(this.datasetTagMap);
        when(this.tagRepository.findByTagIdIn(this.tagIds)).thenReturn(this.tagList);
        when(this.tagUtil.getTagsByDatasetId(this.datasetId)).thenReturn(this.tagList);
        final List<Tag> result = this.tagService.getTagsByDatasetId(this.datasetId);
        assertEquals(this.tagList, result);

    }

}
