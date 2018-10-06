package com.paypal.udc.entity.storagetype;

import java.io.Serializable;
import java.util.List;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Transient;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.paypal.udc.entity.Storage;
import io.swagger.annotations.ApiModelProperty;


@Entity
@Table(name = "pc_storage_type")
public class StorageType implements Serializable {

    private static final long serialVersionUID = 1L;

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @ApiModelProperty(notes = "The database generated storage type ID")
    @Column(name = "storage_type_id")
    @NotNull
    private long storageTypeId;

    @ApiModelProperty(notes = "Name of the Storage type")
    @Column(name = "storage_type_name")
    @NotNull
    @Size(min = 1, message = "name need to have atleast 1 character")
    private String storageTypeName;

    @ApiModelProperty(notes = "Description on the Storage type")
    @Column(name = "storage_type_desc")
    @Size(min = 1, message = "Description need to have atleast 1 character")
    private String storageTypeDescription;

    @ApiModelProperty(notes = "Created User")
    @Column(name = "cre_user")
    private String createdUser;

    @ApiModelProperty(notes = "Created Timestamp")
    @Column(name = "cre_ts")
    @JsonIgnore
    private String createdTimestamp;

    @ApiModelProperty(notes = "Updated User")
    @Column(name = "upd_user")
    private String updatedUser;

    @ApiModelProperty(notes = "Updated Timestamp")
    @Column(name = "upd_ts")
    @JsonIgnore
    private String updatedTimestamp;

    @ApiModelProperty(notes = "The database generated storage category ID")
    @Column(name = "storage_id")
    @NotNull
    @JsonIgnore
    private long storageId;

    @ApiModelProperty(notes = "Is the Storage Type active ?")
    @Column(name = "is_active_y_n")
    private String isActiveYN;

    @Transient
    private Storage storage;

    @Transient
    private List<StorageTypeAttributeKey> attributeKeys;

    public String getIsActiveYN() {
        return this.isActiveYN;
    }

    public void setIsActiveYN(final String isActiveYN) {
        this.isActiveYN = isActiveYN;
    }

    public List<StorageTypeAttributeKey> getAttributeKeys() {
        return this.attributeKeys;
    }

    public void setAttributeKeys(final List<StorageTypeAttributeKey> attributeKeys) {
        this.attributeKeys = attributeKeys;
    }

    public Storage getStorage() {
        return this.storage;
    }

    public void setStorage(final Storage storage) {
        this.storage = storage;
    }

    public long getStorageTypeId() {
        return this.storageTypeId;
    }

    public void setStorageTypeId(final long storageTypeId) {
        this.storageTypeId = storageTypeId;
    }

    public String getStorageTypeName() {
        return this.storageTypeName;
    }

    public void setStorageTypeName(final String storageTypeName) {
        this.storageTypeName = storageTypeName;
    }

    public String getStorageTypeDescription() {
        return this.storageTypeDescription;
    }

    public void setStorageTypeDescription(final String storageTypeDescription) {
        this.storageTypeDescription = storageTypeDescription;
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

    @JsonIgnore
    public long getStorageId() {
        return this.storageId;
    }

    @JsonProperty
    public void setStorageId(final long storageId) {
        this.storageId = storageId;
    }

    public StorageType() {

    }

    public StorageType(final String storageTypeName, final String storageTypeDescription,
            final String createdUser,
            final String createdTimestamp, final String updatedUser, final String updatedTimestamp,
            final long storageId) {
        super();
        this.storageTypeName = storageTypeName;
        this.storageTypeDescription = storageTypeDescription;
        this.createdUser = createdUser;
        this.createdTimestamp = createdTimestamp;
        this.updatedUser = updatedUser;
        this.updatedTimestamp = updatedTimestamp;
        this.storageId = storageId;
    }

    @Override
    public String toString() {
        return "Storage Type ID -> " + this.storageTypeId + " ,Name -> " + this.storageTypeName + " ,Description -> "
                + this.storageTypeDescription + " ,Storage ID -> " + this.storageId;
    }
}
