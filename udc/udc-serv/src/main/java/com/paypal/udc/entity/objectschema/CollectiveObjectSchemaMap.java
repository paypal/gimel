package com.paypal.udc.entity.objectschema;

import java.util.List;


public class CollectiveObjectSchemaMap {

    private long objectId;
    private String objectName;
    private String containerName;
    private long storageSystemId;
    private List<Long> clusterIds;
    private String query;
    private List<Schema> objectSchema;
    private List<CollectiveObjectAttributeValue> objectAttributes;
    private String isActiveYN;
    private String createdTimestampOnStore;
    private String createdUserOnStore;

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

    public long getObjectId() {
        return this.objectId;
    }

    public void setObjectId(final long objectId) {
        this.objectId = objectId;
    }

    public String getObjectName() {
        return this.objectName;
    }

    public void setObjectName(final String objectName) {
        this.objectName = objectName;
    }

    public String getContainerName() {
        return this.containerName;
    }

    public void setContainerName(final String containerName) {
        this.containerName = containerName;
    }

    public long getStorageSystemId() {
        return this.storageSystemId;
    }

    public void setStorageSystemId(final long storageSystemId) {
        this.storageSystemId = storageSystemId;
    }

    public CollectiveObjectSchemaMap(final long objectId, final String objectName, final String containerName,
            final long storageSystemId, final List<Long> clusterIds, final String query,
            final List<Schema> objectSchema,
            final List<CollectiveObjectAttributeValue> objectAttributes, final String isActiveYN,
            final String createdUserOnStore, final String createdTimestampOnStore) {
        this.objectId = objectId;
        this.objectName = objectName;
        this.containerName = containerName;
        this.storageSystemId = storageSystemId;
        this.clusterIds = clusterIds;
        this.query = query;
        this.objectSchema = objectSchema;
        this.objectAttributes = objectAttributes;
        this.isActiveYN = isActiveYN;
        this.createdUserOnStore = createdUserOnStore;
        this.createdTimestampOnStore = createdTimestampOnStore;
    }

    public List<Long> getClusterIds() {
        return this.clusterIds;
    }

    public void setClusterIds(final List<Long> clusterIds) {
        this.clusterIds = clusterIds;
    }

    public String getQuery() {
        return this.query;
    }

    public void setQuery(final String query) {
        this.query = query;
    }

    public List<Schema> getObjectSchema() {
        return this.objectSchema;
    }

    public void setObjectSchema(final List<Schema> objectSchema) {
        this.objectSchema = objectSchema;
    }

    public List<CollectiveObjectAttributeValue> getObjectAttributes() {
        return this.objectAttributes;
    }

    public void setObjectAttributes(final List<CollectiveObjectAttributeValue> objectAttributes) {
        this.objectAttributes = objectAttributes;
    }

}
