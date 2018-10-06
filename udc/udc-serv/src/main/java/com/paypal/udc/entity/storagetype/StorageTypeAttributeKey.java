package com.paypal.udc.entity.storagetype;

import java.io.Serializable;
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
import io.swagger.annotations.ApiModelProperty;


@Entity
@Table(name = "pc_storage_type_attribute_key")
public class StorageTypeAttributeKey implements Serializable {

    private static final long serialVersionUID = 1L;

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @ApiModelProperty(notes = "The database generated attribute key ID")
    @Column(name = "storage_ds_attribute_key_id")
    @NotNull
    private long storageDsAttributeKeyId;

    @ApiModelProperty(notes = "Name of the Attribute key")
    @Column(name = "storage_ds_attribute_key_name")
    @NotNull
    @Size(min = 1, message = "name need to have atleast 1 character")
    private String storageDsAttributeKeyName;

    @ApiModelProperty(notes = "Description on the Attribute Key")
    @Column(name = "storage_ds_attribute_key_desc")
    @Size(min = 1, message = "name need to have atleast 1 character")
    @NotNull
    private String storageDsAttributeKeyDesc;

    @ApiModelProperty(notes = "Is this attribute at Storage System Level ?")
    @Column(name = "is_storage_system_level")
    @NotNull
    private String isStorageSystemLevel;

    @ApiModelProperty(notes = "Created User")
    @Column(name = "cre_user")
    @JsonIgnore
    private String createdUser;

    @ApiModelProperty(notes = "Created Timestamp")
    @Column(name = "cre_ts")
    @JsonIgnore
    private String createdTimestamp;

    @ApiModelProperty(notes = "Updated User")
    @Column(name = "upd_user")
    @JsonIgnore
    private String updatedUser;

    @ApiModelProperty(notes = "Updated Timestamp")
    @Column(name = "upd_ts")
    @JsonIgnore
    private String updatedTimestamp;

    @ApiModelProperty(notes = "The database generated storage type ID")
    @Column(name = "storage_type_id")
    private long storageTypeId;

    @ApiModelProperty(notes = "Is the Storage Category active ?")
    @Column(name = "is_active_y_n")
    private String isActiveYN;

    @Transient
    private String storageTypeAttributeValue;

    public String getStorageTypeAttributeValue() {
        return this.storageTypeAttributeValue;
    }

    public void setStorageTypeAttributeValue(final String storageTypeAttributeValue) {
        this.storageTypeAttributeValue = storageTypeAttributeValue;
    }

    public StorageTypeAttributeKey() {

    }

    @Override
    public String toString() {
        return "Type Attribute Key = " + this.storageDsAttributeKeyId + " Type Attribute Name = "
                + this.storageDsAttributeKeyName;

    }

    public StorageTypeAttributeKey(final String storageDsAttributeKeyName,
            final String storageDsAttributeKeyDesc, final String createdUser, final String createdTimestamp,
            final String updatedUser,
            final String updatedTimestamp, final long storageTypeId, final String isActiveYN,
            final String isStorageSystemLevel) {
        this.storageDsAttributeKeyName = storageDsAttributeKeyName;
        this.storageDsAttributeKeyDesc = storageDsAttributeKeyDesc;
        this.createdUser = createdUser;
        this.createdTimestamp = createdTimestamp;
        this.updatedUser = updatedUser;
        this.updatedTimestamp = updatedTimestamp;
        this.storageTypeId = storageTypeId;
        this.isActiveYN = isActiveYN;
        this.isStorageSystemLevel = isStorageSystemLevel;
    }

    public String getIsActiveYN() {
        return this.isActiveYN;
    }

    public void setIsActiveYN(final String isActiveYN) {
        this.isActiveYN = isActiveYN;
    }

    public String getIsStorageSystemLevel() {
        return this.isStorageSystemLevel;
    }

    public void setIsStorageSystemLevel(final String isStorageSystemLevel) {
        this.isStorageSystemLevel = isStorageSystemLevel;
    }

    public long getStorageDsAttributeKeyId() {
        return this.storageDsAttributeKeyId;
    }

    public void setStorageDsAttributeKeyId(final long storageDsAttributeKeyId) {
        this.storageDsAttributeKeyId = storageDsAttributeKeyId;
    }

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

    @JsonIgnore
    public String getCreatedUser() {
        return this.createdUser;
    }

    @JsonProperty
    public void setCreatedUser(final String createdUser) {
        this.createdUser = createdUser;
    }

    @JsonIgnore
    public String getCreatedTimestamp() {
        return this.createdTimestamp;
    }

    @JsonProperty
    public void setCreatedTimestamp(final String createdTimestamp) {
        this.createdTimestamp = createdTimestamp;
    }

    @JsonIgnore
    public String getUpdatedUser() {
        return this.updatedUser;
    }

    @JsonProperty
    public void setUpdatedUser(final String updatedUser) {
        this.updatedUser = updatedUser;
    }

    @JsonIgnore
    public String getUpdatedTimestamp() {
        return this.updatedTimestamp;
    }

    @JsonProperty
    public void setUpdatedTimestamp(final String updatedTimestamp) {
        this.updatedTimestamp = updatedTimestamp;
    }

    public long getStorageTypeId() {
        return this.storageTypeId;
    }

    public void setStorageTypeId(final long storageTypeId) {
        this.storageTypeId = storageTypeId;
    }

}
