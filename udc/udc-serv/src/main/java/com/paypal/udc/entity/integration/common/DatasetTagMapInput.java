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

package com.paypal.udc.entity.integration.common;

public class DatasetTagMapInput {

    public String tagName;
    public String providerName;
    public String createdUser;
    public long datasetId;

    public String getTagName() {
        return this.tagName;
    }

    public void setTagName(final String tagName) {
        this.tagName = tagName;
    }

    public String getProviderName() {
        return this.providerName;
    }

    public void setProviderName(final String providerName) {
        this.providerName = providerName;
    }

    public String getCreatedUser() {
        return this.createdUser;
    }

    public void setCreatedUser(final String createdUser) {
        this.createdUser = createdUser;
    }

    public long getDatasetId() {
        return this.datasetId;
    }

    public void setDatasetId(final long datasetId) {
        this.datasetId = datasetId;
    }

    public DatasetTagMapInput() {

    }

    public DatasetTagMapInput(final String tagName, final String providerName, final String createdUser,
            final long datasetId) {
        this.tagName = tagName;
        this.providerName = providerName;
        this.createdUser = createdUser;
        this.datasetId = datasetId;
    }

}
