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


public class Hits {
    private long total;
    @SerializedName(value = "max_score")
    private double maxScore;
    private String[] hits;

    public long getTotal() {
        return this.total;
    }

    public void setTotal(final long total) {
        this.total = total;
    }

    public double getMaxScore() {
        return this.maxScore;
    }

    public void setMaxScore(final double maxScore) {
        this.maxScore = maxScore;
    }

    public String[] getHits() {
        return this.hits;
    }

    public void setHits(final String[] hits) {
        this.hits = hits;
    }

    public Hits(final long total, final double maxScore, final String[] hits) {
        super();
        this.total = total;
        this.maxScore = maxScore;
        this.hits = hits;
    }

}
