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

import java.util.List;


public class DatasetDescription {

    private String datasetName;
    private String datasetComment;
    private List<DatasetColumnDescription> datasetColumnDescriptions;
    private String providerName;

    public String getProviderName() {
        return this.providerName;
    }

    public void setProviderName(final String providerName) {
        this.providerName = providerName;
    }

    public String getDatasetName() {
        return this.datasetName;
    }

    public void setDatasetName(final String datasetName) {
        this.datasetName = datasetName;
    }

    public String getDatasetComment() {
        return this.datasetComment;
    }

    public void setDatasetComment(final String datasetComment) {
        this.datasetComment = datasetComment;
    }

    public List<DatasetColumnDescription> getDatasetColumnDescriptions() {
        return this.datasetColumnDescriptions;
    }

    public void setDatasetColumnDescriptions(final List<DatasetColumnDescription> datasetColumnDescriptions) {
        this.datasetColumnDescriptions = datasetColumnDescriptions;
    }

    public DatasetDescription(final String datasetName, final String datasetComment,
            final List<DatasetColumnDescription> datasetColumnDescriptions, final String providerName) {
        this.datasetName = datasetName;
        this.datasetComment = datasetComment;
        this.datasetColumnDescriptions = datasetColumnDescriptions;
        this.providerName = providerName;
    }

}
