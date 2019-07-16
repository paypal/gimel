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

package com.paypal.udc.entity.dataset;

import java.util.List;


public class CumulativeDataset {

    private long storageTypeId;
    private String storageTypeName;
    private List<Dataset> datasets;

    public CumulativeDataset(final long storageTypeId, final String storageTypeName, final List<Dataset> datasets) {
        this.storageTypeId = storageTypeId;
        this.storageTypeName = storageTypeName;
        this.datasets = datasets;
    }

    public long getStorageTypeId() {
        return this.storageTypeId;
    }

    public void setStorageTypeId(final long storageTypeId) {
        this.storageTypeId = storageTypeId;
    }

    public String getStorageTypeName() {
        return this.storageTypeName;
    }

    public void setStorageTypeName(final String storageTypeName) {
        this.storageTypeName = storageTypeName;
    }

    public List<Dataset> getDatasets() {
        return this.datasets;
    }

    public void setDatasets(final List<Dataset> datasets) {
        this.datasets = datasets;
    }

}
