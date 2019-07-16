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

package com.paypal.udc.entity.cluster;

public class ClusterAttributeKeyValue {

    private String clusterAttributeKeyName;
    private String clusterAttributeValue;
    private String clusterAttributeKeyType;
    private long clusterAttributeKeyId;
    private long clusterAttributeValueId;

    public ClusterAttributeKeyValue() {

    }

    public ClusterAttributeKeyValue(final String clusterAttributeKeyName, final String clusterAttributeValue,
            final String clusterAttributeKeyType, final long clusterAttributeKeyId,
            final long clusterAttributeValueId) {
        this.clusterAttributeKeyName = clusterAttributeKeyName;
        this.clusterAttributeValue = clusterAttributeValue;
        this.clusterAttributeKeyId = clusterAttributeKeyId;
        this.clusterAttributeValueId = clusterAttributeValueId;
        this.clusterAttributeKeyType = clusterAttributeKeyType;
    }

    public String getClusterAttributeKeyType() {
        return this.clusterAttributeKeyType;
    }

    public void setClusterAttributeKeyType(final String clusterAttributeKeyType) {
        this.clusterAttributeKeyType = clusterAttributeKeyType;
    }

    public long getClusterAttributeKeyId() {
        return this.clusterAttributeKeyId;
    }

    public void setClusterAttributeKeyId(final long clusterAttributeKeyId) {
        this.clusterAttributeKeyId = clusterAttributeKeyId;
    }

    public long getClusterAttributeValueId() {
        return this.clusterAttributeValueId;
    }

    public void setClusterAttributeValueId(final long clusterAttributeValueId) {
        this.clusterAttributeValueId = clusterAttributeValueId;
    }

    public String getClusterAttributeKeyName() {
        return this.clusterAttributeKeyName;
    }

    public void setClusterAttributeKeyName(final String clusterAttributeKeyName) {
        this.clusterAttributeKeyName = clusterAttributeKeyName;
    }

    public String getClusterAttributeValue() {
        return this.clusterAttributeValue;
    }

    public void setClusterAttributeValue(final String clusterAttributeValue) {
        this.clusterAttributeValue = clusterAttributeValue;
    }

}
