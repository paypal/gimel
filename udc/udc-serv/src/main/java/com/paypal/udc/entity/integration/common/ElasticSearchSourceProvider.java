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

import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;


@Document(indexName = "#{@providerIndexName}", type = "udc")
public class ElasticSearchSourceProvider {

    @Id
    private String _id;
    private long sourceProviderId;
    private String sourceProviderName;
    private String sourceProviderDescription;
    private String createdUser;
    private String createdTimestamp;
    private String updatedUser;
    private String updatedTimestamp;

    public ElasticSearchSourceProvider() {

    }

    public ElasticSearchSourceProvider(final long sourceProviderId, final String sourceProviderName,
            final String sourceProviderDescription, final String createdUser, final String createdTimestamp,
            final String updatedUser,
            final String updatedTimestamp) {
        super();
        this.sourceProviderId = sourceProviderId;
        this.sourceProviderName = sourceProviderName;
        this.sourceProviderDescription = sourceProviderDescription;
        this.createdUser = createdUser;
        this.createdTimestamp = createdTimestamp;
        this.updatedUser = updatedUser;
        this.updatedTimestamp = updatedTimestamp;
    }

    public String get_id() {
        return this._id;
    }

    public void set_id(final String _id) {
        this._id = _id;
    }

    public long getSourceProviderId() {
        return this.sourceProviderId;
    }

    public void setSourceProviderId(final long sourceProviderId) {
        this.sourceProviderId = sourceProviderId;
    }

    public String getSourceProviderName() {
        return this.sourceProviderName;
    }

    public void setSourceProviderName(final String sourceProviderName) {
        this.sourceProviderName = sourceProviderName;
    }

    public String getSourceProviderDescription() {
        return this.sourceProviderDescription;
    }

    public void setSourceProviderDescription(final String sourceProviderDescription) {
        this.sourceProviderDescription = sourceProviderDescription;
    }

    public String getCreatedUser() {
        return this.createdUser;
    }

    public void setCreatedUser(final String createdUser) {
        this.createdUser = createdUser;
    }

    public String getCreatedTimestamp() {
        return this.createdTimestamp;
    }

    public void setCreatedTimestamp(final String createdTimestamp) {
        this.createdTimestamp = createdTimestamp;
    }

    public String getUpdatedUser() {
        return this.updatedUser;
    }

    public void setUpdatedUser(final String updatedUser) {
        this.updatedUser = updatedUser;
    }

    public String getUpdatedTimestamp() {
        return this.updatedTimestamp;
    }

    public void setUpdatedTimestamp(final String updatedTimestamp) {
        this.updatedTimestamp = updatedTimestamp;
    }

}
