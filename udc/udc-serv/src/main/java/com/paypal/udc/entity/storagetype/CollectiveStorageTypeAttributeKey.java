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
