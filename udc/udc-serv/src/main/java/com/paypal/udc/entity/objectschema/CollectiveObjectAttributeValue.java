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

package com.paypal.udc.entity.objectschema;

public class CollectiveObjectAttributeValue {

    private long storageDsAttributeKeyId;
    private String objectAttributeValue;
    private long objectId;
    private String isCustomized;

    public String getIsCustomized() {
        return this.isCustomized;
    }

    public void setIsCustomized(final String isCustomized) {
        this.isCustomized = isCustomized;
    }

    public long getObjectId() {
        return this.objectId;
    }

    public void setObjectId(final long objectId) {
        this.objectId = objectId;
    }

    public long getStorageDsAttributeKeyId() {
        return this.storageDsAttributeKeyId;
    }

    public void setStorageDsAttributeKeyId(final long storageDsAttributeKeyId) {
        this.storageDsAttributeKeyId = storageDsAttributeKeyId;
    }

    public String getObjectAttributeValue() {
        return this.objectAttributeValue;
    }

    public void setObjectAttributeValue(final String objectAttributeValue) {
        this.objectAttributeValue = objectAttributeValue;
    }

}
