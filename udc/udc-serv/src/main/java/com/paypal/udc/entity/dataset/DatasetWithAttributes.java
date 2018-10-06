package com.paypal.udc.entity.dataset;

import java.util.List;
import com.paypal.udc.entity.objectschema.ObjectAttributeValue;
import com.paypal.udc.entity.objectschema.Schema;
import com.paypal.udc.entity.storagesystem.StorageSystemAttributeValue;
import com.paypal.udc.entity.storagetype.StorageTypeAttributeKey;


public class DatasetWithAttributes {

    private long storageDataSetId;
    private String storageDataSetName;
    private long objectSchemaMapId;
    private String storageDataSetAliasName;
    private String isAutoRegistered;
    private String createdUser;
    private String createdTimestamp;
    private String updatedUser;
    private String updatedTimestamp;
    private long storageSystemId;
    private String query;
    private String isActiveYN;
    private List<Schema> objectSchema;
    private String storageSystemName;
    private List<StorageSystemAttributeValue> systemAttributes;
    private List<ObjectAttributeValue> objectAttributes;
    private List<StorageTypeAttributeKey> pendingTypeAttributes;
    private String createdTimestampOnStore;
    private String createdUserOnStore;

    public DatasetWithAttributes(final long storageDataSetId, final String storageDataSetName,
            final long objectSchemaMapId, final String storageDataSetAliasName, final String isAutoRegistered,
            final String createdUser, final String createdTimestamp, final String updatedUser,
            final String updatedTimestamp, final long storageSystemId, final String query,
            final String isActiveYN, final String storageSystemName, final String createdTimestampOnStore,
            final String createdUserOnStore) {
        this.storageDataSetId = storageDataSetId;
        this.storageDataSetName = storageDataSetName;
        this.objectSchemaMapId = objectSchemaMapId;
        this.storageDataSetAliasName = storageDataSetAliasName;
        this.isAutoRegistered = isAutoRegistered;
        this.createdUser = createdUser;
        this.createdTimestamp = createdTimestamp;
        this.updatedUser = updatedUser;
        this.updatedTimestamp = updatedTimestamp;
        this.storageSystemId = storageSystemId;
        this.query = query;
        this.isActiveYN = isActiveYN;
        this.storageSystemName = storageSystemName;
        this.createdTimestampOnStore = createdTimestampOnStore;
        this.createdUserOnStore = createdUserOnStore;
    }

    public DatasetWithAttributes() {

    }

    public String getCreatedTimestampOnStore() {
        return this.createdTimestampOnStore;
    }

    public void setCreatedTimestampOnStore(final String createdTimestampOnStore) {
        this.createdTimestampOnStore = createdTimestampOnStore;
    }

    public String getCreatedUserOnStore() {
        return this.createdUserOnStore;
    }

    public void setCreatedUserOnStore(final String createdUserOnStore) {
        this.createdUserOnStore = createdUserOnStore;
    }

    public String getIsActiveYN() {
        return this.isActiveYN;
    }

    public void setIsActiveYN(final String isActiveYN) {
        this.isActiveYN = isActiveYN;
    }

    public String getStorageDataSetAliasName() {
        return this.storageDataSetAliasName;
    }

    public void setStorageDataSetAliasName(final String storageDataSetAliasName) {
        this.storageDataSetAliasName = storageDataSetAliasName;
    }

    public long getStorageDataSetId() {
        return this.storageDataSetId;
    }

    public void setStorageDataSetId(final long storageDataSetId) {
        this.storageDataSetId = storageDataSetId;
    }

    public String getStorageDataSetName() {
        return this.storageDataSetName;
    }

    public void setStorageDataSetName(final String storageDataSetName) {
        this.storageDataSetName = storageDataSetName;
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

    public List<ObjectAttributeValue> getObjectAttributes() {
        return this.objectAttributes;
    }

    public void setObjectAttributes(final List<ObjectAttributeValue> objectAttributes) {
        this.objectAttributes = objectAttributes;
    }

    public List<StorageSystemAttributeValue> getSystemAttributes() {
        return this.systemAttributes;
    }

    public void setSystemAttributes(final List<StorageSystemAttributeValue> systemAttributes) {
        this.systemAttributes = systemAttributes;
    }

    public String getQuery() {
        return this.query;
    }

    public void setQuery(final String query) {
        this.query = query;
    }

    public long getObjectSchemaMapId() {
        return this.objectSchemaMapId;
    }

    public void setObjectSchemaMapId(final long objectSchemaMapId) {
        this.objectSchemaMapId = objectSchemaMapId;
    }

    public List<Schema> getObjectSchema() {
        return this.objectSchema;
    }

    public void setObjectSchema(final List<Schema> objectSchema) {
        this.objectSchema = objectSchema;
    }

    public String getIsAutoRegistered() {
        return this.isAutoRegistered;
    }

    public void setIsAutoRegistered(final String isAutoRegistered) {
        this.isAutoRegistered = isAutoRegistered;
    }

    public String getCreatedUser() {
        return this.createdUser;
    }

    public void setCreatedUser(final String createdUser) {
        this.createdUser = createdUser;
    }

    public String getCreatedTimestamp() {
        return this.createdTimestamp;
    }

    public void setCreatedTimestamp(final String createdTimestamp) {
        this.createdTimestamp = createdTimestamp;
    }

    public String getUpdatedUser() {
        return this.updatedUser;
    }

    public void setUpdatedUser(final String updatedUser) {
        this.updatedUser = updatedUser;
    }

    public String getUpdatedTimestamp() {
        return this.updatedTimestamp;
    }

    public void setUpdatedTimestamp(final String updatedTimestamp) {
        this.updatedTimestamp = updatedTimestamp;
    }

    public List<StorageTypeAttributeKey> getPendingTypeAttributes() {
        return this.pendingTypeAttributes;
    }

    public void setPendingTypeAttributes(final List<StorageTypeAttributeKey> pendingTypeAttributes) {
        this.pendingTypeAttributes = pendingTypeAttributes;
    }

}
