package com.paypal.udc.entity;

import java.io.Serializable;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModelProperty;


@Entity
@Table(name = "pc_storage")
public class Storage implements Serializable {

    private static final long serialVersionUID = 1L;

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @ApiModelProperty(notes = "The database generated storage category ID")
    @Column(name = "storage_id")
    @NotNull
    private long storageId;

    @ApiModelProperty(notes = "Name of the Storage Category")
    @Column(name = "storage_name")
    @Size(min = 1, message = "name need to have atleast 1 character")
    @NotNull
    private String storageName;

    @ApiModelProperty(notes = "Description on the Storage Category")
    @Column(name = "storage_desc")
    @Size(min = 1, message = "name need to have atleast 1 character")
    private String storageDescription;

    @ApiModelProperty(notes = "Is the Storage Category active ?")
    @Column(name = "is_active_y_n")
    private String isActiveYN;

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

    public String getIsActiveYN() {
        return this.isActiveYN;
    }

    public void setIsActiveYN(final String isActiveYN) {
        this.isActiveYN = isActiveYN;
    }

    public static long getSerialversionuid() {
        return serialVersionUID;
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

    public String getCreatedUser() {
        return this.createdUser;
    }

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

    public String getUpdatedTimestamp() {
        return this.updatedTimestamp;
    }

    public void setUpdatedTimestamp(final String updatedTimestamp) {
        this.updatedTimestamp = updatedTimestamp;
    }

    public Storage() {

    }

    public Storage(final long storageId, final String storageName, final String storageDescription,
            final String createdUser,
            final String createdTimestamp, final String updatedUser, final String updatedTimestamp) {
        super();
        this.storageId = storageId;
        this.storageName = storageName;
        this.storageDescription = storageDescription;
        this.createdUser = createdUser;
        this.createdTimestamp = createdTimestamp;
        this.updatedUser = updatedUser;
        this.updatedTimestamp = updatedTimestamp;
    }

    @Override
    public String toString() {
        return "Storage Category ID -> " + this.storageId + ",Name -> " + this.storageName + ",Description -> "
                + this.storageDescription + ",Active ->" + this.isActiveYN;
    }

}
