

package com.paypal.udc.entity.storagecategory;

import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;


@Document(indexName = "#{@categoryIndexName}", type = "udc")
public class ElasticSearchStorage {

    @Id
    private String _id;
    private long storageId;
    private String storageName;
    private String storageDescription;
    private String isActiveYN;
    private String createdUser;
    private String createdTimestamp;
    private String updatedUser;
    private String updatedTimestamp;

    public String get_id() {
        return this._id;
    }

    public void set_id(final String _id) {
        this._id = _id;
    }

    public long getStorageId() {
        return this.storageId;
    }

    public void setStorageId(final long storageId) {
        this.storageId = storageId;
    }

    public String getStorageName() {
        return this.storageName;
    }

    public void setStorageName(final String storageName) {
        this.storageName = storageName;
    }

    public String getStorageDescription() {
        return this.storageDescription;
    }

    public void setStorageDescription(final String storageDescription) {
        this.storageDescription = storageDescription;
    }

    public String getIsActiveYN() {
        return this.isActiveYN;
    }

    public void setIsActiveYN(final String isActiveYN) {
        this.isActiveYN = isActiveYN;
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

    public ElasticSearchStorage() {

    }

    public ElasticSearchStorage(final long storageId, final String storageName, final String storageDescription,
            final String isActiveYN, final String createdUser, final String createdTimestamp, final String updatedUser,
            final String updatedTimestamp) {
        this.storageId = storageId;
        this.storageName = storageName;
        this.storageDescription = storageDescription;
        this.isActiveYN = isActiveYN;
        this.createdUser = createdUser;
        this.createdTimestamp = createdTimestamp;
        this.updatedUser = updatedUser;
        this.updatedTimestamp = updatedTimestamp;
    }

}
