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

package com.paypal.udc.entity.storagesystem;

import java.util.List;
import com.paypal.udc.entity.storagetype.StorageTypeAttributeKey;


public class CollectiveStorageSystemContainerObject {

    private long storageSystemId;
    private String storageSystemName;
    private String containerName;
    List<StorageSystemAttributeValue> systemAttributes;
    List<StorageTypeAttributeKey> typeAttributes;
    private String userName;

    public String getUserName() {
        return this.userName;
    }

    public void setUserName(final String userName) {
        this.userName = userName;
    }

    public List<StorageTypeAttributeKey> getTypeAttributes() {
        return this.typeAttributes;
    }

    public void setTypeAttributes(final List<StorageTypeAttributeKey> typeAttributes) {
        this.typeAttributes = typeAttributes;
    }

    public long getStorageSystemId() {
        return this.storageSystemId;
    }

    public void setStorageSystemId(final long storageSystemId) {
        this.storageSystemId = storageSystemId;
    }

    public String getStorageSystemName() {
        return this.storageSystemName;
    }

    public void setStorageSystemName(final String storageSystemName) {
        this.storageSystemName = storageSystemName;
    }

    public String getContainerName() {
        return this.containerName;
    }

    public void setContainerName(final String containerName) {
        this.containerName = containerName;
    }

    public List<StorageSystemAttributeValue> getSystemAttributes() {
        return this.systemAttributes;
    }

    public void setSystemAttributes(final List<StorageSystemAttributeValue> systemAttributes) {
        this.systemAttributes = systemAttributes;
    }

}
