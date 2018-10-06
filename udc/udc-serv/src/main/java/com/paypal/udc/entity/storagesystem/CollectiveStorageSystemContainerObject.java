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
