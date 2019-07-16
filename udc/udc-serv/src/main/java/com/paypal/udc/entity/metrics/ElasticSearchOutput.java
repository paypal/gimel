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

package com.paypal.udc.entity.metrics;

import com.google.gson.annotations.SerializedName;


public class ElasticSearchOutput {

    private int took;
    @SerializedName(value = "timed_out")
    private boolean timedOut;
    @SerializedName(value = "_shards")
    private Shards shards;
    private Hits hits;
    private OutputAggregate aggregations;

    public int getTook() {
        return this.took;
    }

    public void setTook(final int took) {
        this.took = took;
    }

    public boolean isTimedOut() {
        return this.timedOut;
    }

    public void setTimedOut(final boolean timedOut) {
        this.timedOut = timedOut;
    }

    public Shards getShards() {
        return this.shards;
    }

    public void setShards(final Shards shards) {
        this.shards = shards;
    }

    public Hits getHits() {
        return this.hits;
    }

    public void setHits(final Hits hits) {
        this.hits = hits;
    }

    public OutputAggregate getAggregations() {
        return this.aggregations;
    }

    public void setAggregations(final OutputAggregate aggregations) {
        this.aggregations = aggregations;
    }

}
