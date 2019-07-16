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

package com.paypal.udc.service;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import com.paypal.udc.entity.integration.common.DatasetTagMapInput;
import com.paypal.udc.entity.integration.common.Tag;
import com.paypal.udc.exception.ValidationError;


public interface ITagService {

    List<Tag> getAllTags();

    Tag getTagById(long tagId) throws ValidationError;

    Tag addTag(Tag tag) throws ValidationError;

    Tag updateTag(Tag tag) throws ValidationError;

    Tag getTagByName(String tagName);

    List<Tag> getByProviderId(long providerId);

    List<Tag> getByTagIds(String ids);

    List<Tag> getTagsByDatasetId(long datasetId);

    void addTagForDataset(final DatasetTagMapInput datasetTagMapInput)
            throws ValidationError, IOException, InterruptedException, ExecutionException;

}
