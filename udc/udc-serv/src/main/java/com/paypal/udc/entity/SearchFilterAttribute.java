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

package com.paypal.udc.entity;

import java.util.List;


public class SearchFilterAttribute {

    public List<Long> ids;
    public boolean isFilter;
    public String filterType;

    @Override
    public String toString() {
        return "SearchFilterAttribute [ids=" + this.ids + ", isFilter=" + this.isFilter + ", filterType="
                + this.filterType + "]";
    }

    public SearchFilterAttribute(final List<Long> ids, final boolean isFilter, final String filterType) {
        this.ids = ids;
        this.isFilter = isFilter;
        this.filterType = filterType;
    }

    public String getFilterType() {
        return this.filterType;
    }

    public void setFilterType(final String filterType) {
        this.filterType = filterType;
    }

    public List<Long> getIds() {
        return this.ids;
    }

    public void setIds(final List<Long> ids) {
        this.ids = ids;
    }

    public boolean isFilter() {
        return this.isFilter;
    }

    public void setFilter(final boolean isFilter) {
        this.isFilter = isFilter;
    }

}
