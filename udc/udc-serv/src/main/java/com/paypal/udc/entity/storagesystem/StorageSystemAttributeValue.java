package com.paypal.udc.entity.storagesystem;

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
@Table(name = "pc_storage_system_attribute_value")
public class StorageSystemAttributeValue implements Serializable {

    private static final long serialVersionUID = 1L;
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @ApiModelProperty(notes = "The database generated storage system attribute ID")
    @Column(name = "storage_system_attribute_value_id")
    @NotNull
    private long storageSystemAttributeValueId;

    @ApiModelProperty(notes = "Name of the Storage system attribute value")
    @Column(name = "storage_system_attribute_value")
    @NotNull
    @Size(min = 1, message = "Value need to have atleast 1 character")
    private String storageSystemAttributeValue;

    @ApiModelProperty(notes = "Attribute Key ID")
    @Column(name = "storage_ds_attribute_key_id")
    @NotNull
    private long storageDataSetAttributeKeyId;

    @Transient
    private String storageDsAttributeKeyName;

    @ApiModelProperty(notes = "StorageSystemId")
    @Column(name = "storage_system_id")
    @NotNull
    private long storageSystemId;

    @ApiModelProperty(notes = "Is the Storage Type active ?")
    @Column(name = "is_active_y_n")
    @JsonIgnore
    private String isActiveYN;

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

    public String getIsActiveYN() {
        return this.isActiveYN;
    }

    public void setIsActiveYN(final String isActiveYN) {
        this.isActiveYN = isActiveYN;
    }

    public long getStorageSystemAttributeValueId() {
        return this.storageSystemAttributeValueId;
    }

    public void setStorageSystemAttributeValueId(final long storageSystemAttributeValueId) {
        this.storageSystemAttributeValueId = storageSystemAttributeValueId;
    }

    public String getStorageSystemAttributeValue() {
        return this.storageSystemAttributeValue;
    }

    public void setStorageSystemAttributeValue(final String storageSystemAttributeValue) {
        this.storageSystemAttributeValue = storageSystemAttributeValue;
    }

    public long getStorageDataSetAttributeKeyId() {
        return this.storageDataSetAttributeKeyId;
    }

    public void setStorageDataSetAttributeKeyId(final long storageDataSetAttributeKeyId) {
        this.storageDataSetAttributeKeyId = storageDataSetAttributeKeyId;
    }

    public long getStorageSystemID() {
        return this.storageSystemId;
    }

    public void setStorageSystemID(final long storageSystemId) {
        this.storageSystemId = storageSystemId;
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

    public String getStorageDsAttributeKeyName() {
        return this.storageDsAttributeKeyName;
    }

    public void setStorageDsAttributeKeyName(final String storageDsAttributeKeyName) {
        this.storageDsAttributeKeyName = storageDsAttributeKeyName;
    }

}
