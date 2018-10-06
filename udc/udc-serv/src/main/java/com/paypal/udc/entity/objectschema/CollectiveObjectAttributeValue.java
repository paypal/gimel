package com.paypal.udc.entity.objectschema;

public class CollectiveObjectAttributeValue {

    private long storageDsAttributeKeyId;
    private String objectAttributeValue;
    private long objectId;

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
