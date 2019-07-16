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

package com.paypal.udc.controller;

import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import java.util.Arrays;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;
import com.google.gson.Gson;
import com.paypal.udc.entity.integration.common.Tag;
import com.paypal.udc.service.ITagService;


@RunWith(SpringRunner.class)
@WebMvcTest(TagController.class)
public class TagControllerTest {

    private MockMvc mockMvc;

    @MockBean
    private ITagService tagService;

    @Autowired
    private WebApplicationContext webApplicationContext;

    final Gson gson = new Gson();

    private long tagId, tagIdUpd, providerId, storageDatasetId;
    private String ids;
    private String tagName, tagNameUpd;
    private Tag tag, tagUpd;
    private List<Tag> tagList;
    private String jsonTag;

    class AnyTag implements ArgumentMatcher<Tag> {
        @Override
        public boolean matches(final Tag tag) {
            return tag instanceof Tag;
        }
    }

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        this.mockMvc = MockMvcBuilders.webAppContextSetup(this.webApplicationContext).build();
        this.ids = "1";
        this.tagId = 1L;
        this.tagIdUpd = 2L;
        this.providerId = 1L;
        this.storageDatasetId = 1L;
        this.tagName = "tag1";
        this.tagNameUpd = "tag2";
        this.tag = new Tag(this.tagName, this.providerId, "CrUser", "CrTime",
                "UdpUser", "UpdTime");
        this.tag.setTagId(this.tagId);
        this.tagUpd = new Tag(this.tagNameUpd, this.providerId,
                "CrUser", "CrTime", "UpdUser", "UpdTime");
        this.tagUpd.setTagId(this.tagIdUpd);

        this.tagList = Arrays.asList(this.tag, this.tagUpd);

        this.jsonTag = "{" +
                "\"providerId\": 1, " +
                "\"tagName\": \"tag1\", " +
                "\"tagId\": 1, " +
                "\"createdTimestamp\": \"CrTime\", " +
                "\"createdUser\": \"CrUser\", " +
                "\"updatedUser\": \"UpdUser\"" +
                "}";
    }

    @Test
    public void verifyValidGetTagById() throws Exception {
        when(this.tagService.getTagById(this.tagId))
                .thenReturn(this.tag);

        this.mockMvc.perform(get("/tag/tag/{id}", this.tagId)
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.tagId").value(this.tagId));

        verify(this.tagService).getTagById(this.tagId);
    }

    @Test
    public void verifyValidGetTagByName() throws Exception {
        when(this.tagService.getTagByName(this.tagName))
                .thenReturn(this.tag);

        this.mockMvc.perform(get("/tag/tagByName/{name:.+}", this.tagName)
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.tagName").value(this.tagName));

        verify(this.tagService).getTagByName(this.tagName);
    }

    @Test
    public void verifyValidGetAllTags() throws Exception {
        when(this.tagService.getAllTags())
                .thenReturn(this.tagList);

        this.mockMvc.perform(get("/tag/tags/")
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(this.tagList.size())));

        verify(this.tagService).getAllTags();
    }

    @Test
    public void verifyValidGetAllTagsFromList() throws Exception {
        when(this.tagService.getByTagIds(this.ids))
                .thenReturn(this.tagList);

        this.mockMvc.perform(get("/tag/tags/{ids}", this.ids)
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(this.tagList.size())));

        verify(this.tagService).getByTagIds(this.ids);
    }

    @Test
    public void verifyValidAddTag() throws Exception {
        when(this.tagService.addTag(argThat(new AnyTag())))
                .thenReturn(this.tag);

        this.mockMvc.perform(post("/tag/tag/")
                .contentType(MediaType.APPLICATION_JSON)
                .content(this.jsonTag)
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.tagId").exists())
                .andExpect(jsonPath("$.tagId").value(this.tagId));

        verify(this.tagService).addTag(argThat(new AnyTag()));
    }

    @Test
    public void verifyValidUpdateTag() throws Exception {
        when(this.tagService.updateTag(argThat(new AnyTag())))
                .thenReturn(this.tag);

        this.mockMvc.perform(put("/tag/tag")
                .contentType(MediaType.APPLICATION_JSON)
                .content(this.jsonTag)
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.tagId").exists())
                .andExpect(jsonPath("$.tagId").value(this.tagId));

        verify(this.tagService).updateTag(argThat(new AnyTag()));
    }

    @Test
    public void verifyGetTagsByDatasetId() throws Exception {
        when(this.tagService.getTagsByDatasetId(this.storageDatasetId))
                .thenReturn(this.tagList);

        this.mockMvc.perform(get("/tag/tagsByDataset/{id}", this.storageDatasetId)
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(this.tagList.size())));

        verify(this.tagService).getTagsByDatasetId(this.storageDatasetId);
    }

}
