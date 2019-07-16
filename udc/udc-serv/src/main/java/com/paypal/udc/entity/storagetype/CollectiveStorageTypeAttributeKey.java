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

package com.paypal.udc.entity.storagetype;

public class CollectiveStorageTypeAttributeKey {

    private String storageDsAttributeKeyName;
    private String storageDsAttributeKeyDesc;
    private String isStorageSystemLevel;
    private long storageTypeId;
    private String createdUser;

    public String getStorageDsAttributeKeyName() {
        return this.storageDsAttributeKeyName;
    }

    public void setStorageDsAttributeKeyName(final String storageDsAttributeKeyName) {
        this.storageDsAttributeKeyName = storageDsAttributeKeyName;
    }

    public String getStorageDsAttributeKeyDesc() {
        return this.storageDsAttributeKeyDesc;
    }

    public void setStorageDsAttributeKeyDesc(final String storageDsAttributeKeyDesc) {
        this.storageDsAttributeKeyDesc = storageDsAttributeKeyDesc;
    }

    public String getIsStorageSystemLevel() {
        return this.isStorageSystemLevel;
    }

    public void setIsStorageSystemLevel(final String isStorageSystemLevel) {
        this.isStorageSystemLevel = isStorageSystemLevel;
    }

    public long getStorageTypeId() {
        return this.storageTypeId;
    }

    public void setStorageTypeId(final long storageTypeId) {
        this.storageTypeId = storageTypeId;
    }

    public String getCreatedUser() {
        return this.createdUser;
    }

    public void setCreatedUser(final String createdUser) {
        this.createdUser = createdUser;
    }

}
