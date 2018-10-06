package com.paypal.udc.entity.storagesystem;

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
import com.paypal.udc.entity.storagetype.StorageType;
import io.swagger.annotations.ApiModelProperty;


@Entity
@Table(name = "pc_storage_system")
public class StorageSystem implements Serializable {

    private static final long serialVersionUID = 1L;

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @ApiModelProperty(notes = "The database generated storage system ID")
    @Column(name = "storage_system_id")
    @NotNull
    private long storageSystemId;

    @ApiModelProperty(notes = "Name of the Storage system")
    @Column(name = "storage_system_name")
    @NotNull
    @Size(min = 1, message = "Name need to have atleast 1 character")
    private String storageSystemName;

    @ApiModelProperty(notes = "Description on the Storage system")
    @Column(name = "storage_system_desc")
    private String storageSystemDescription;

    @ApiModelProperty(notes = "Is the Storage System active ?")
    @Column(name = "is_active_y_n")
    private String isActiveYN;

    @ApiModelProperty(notes = "Admin User FK")
    @Column(name = "admin_user")
    private long adminUserId;

    @ApiModelProperty(notes = "Zone Id FK")
    @Column(name = "zone_id")
    private long zoneId;

    @ApiModelProperty(notes = "Is it Data API compatible ?")
    @Column(name = "is_gimel_compatible")
    private String isGimelCompatible;

    @ApiModelProperty(notes = "Is it Data API Read compatible ?")
    @Column(name = "is_read_compatible")
    private String isReadCompatible;

    @ApiModelProperty(notes = "Tied Cluster ID")
    @Column(name = "running_cluster_id")
    private long runningClusterId;

    @ApiModelProperty(notes = "Created User")
    @Column(name = "cre_user")
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

    @ApiModelProperty(notes = "Storage Type ID")
    @Column(name = "storage_type_id")
    @NotNull
    private long storageTypeId;

    @Transient
    private StorageType storageType;

    @Transient
    private String containers;

    @Transient
    private long assignedClusterId;

    @Transient
    private List<StorageSystemAttributeValue> systemAttributeValues;

    @Transient
    private String zoneName;

    public String getIsReadCompatible() {
        return this.isReadCompatible;
    }

    public void setIsReadCompatible(final String isReadCompatible) {
        this.isReadCompatible = isReadCompatible;
    }

    public String getIsGimelCompatible() {
        return this.isGimelCompatible;
    }

    public void setIsGimelCompatible(final String isGimelCompatible) {
        this.isGimelCompatible = isGimelCompatible;
    }

    public String getZoneName() {
        return this.zoneName;
    }

    public void setZoneName(final String zoneName) {
        this.zoneName = zoneName;
    }

    public long getAssignedClusterId() {
        return this.assignedClusterId;
    }

    public void setAssignedClusterId(final long assignedClusterId) {
        this.assignedClusterId = assignedClusterId;
    }

    public long getAdminUserId() {
        return this.adminUserId;
    }

    public void setAdminUserId(final long adminUserId) {
        this.adminUserId = adminUserId;
    }

    public String getContainers() {
        return this.containers;
    }

    public void setContainers(final String containers) {
        this.containers = containers;
    }

    public List<StorageSystemAttributeValue> getSystemAttributeValues() {
        return this.systemAttributeValues;
    }

    public void setSystemAttributeValues(final List<StorageSystemAttributeValue> systemAttributeValues) {
        this.systemAttributeValues = systemAttributeValues;
    }

    public String getIsActiveYN() {
        return this.isActiveYN;
    }

    public void setIsActiveYN(final String isActiveYN) {
        this.isActiveYN = isActiveYN;
    }

    public StorageType getStorageType() {
        return this.storageType;
    }

    public void setStorageType(final StorageType storageType) {
        this.storageType = storageType;
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

    public String getStorageSystemDescription() {
        return this.storageSystemDescription;
    }

    public void setStorageSystemDescription(final String storageSystemDescription) {
        this.storageSystemDescription = storageSystemDescription;
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

    public long getRunningClusterId() {
        return this.runningClusterId;
    }

    public void setRunningClusterId(final long runningClusterId) {
        this.runningClusterId = runningClusterId;
    }

    public long getZoneId() {
        return this.zoneId;
    }

    public void setZoneId(final long zoneId) {
        this.zoneId = zoneId;
    }

    public StorageSystem() {

    }

    public StorageSystem(final long storageSystemId, final String storageSystemName,
            final String storageSystemDescription,
            final String createdUser, final String createdTimestamp, final String updatedUser,
            final String updatedTimestamp,
            final long storageTypeId, final long runningClusterId, final long zoneId, final String isGimelCompatible,
            final String isReadCompatible) {
        super();
        this.storageSystemId = storageSystemId;
        this.storageSystemName = storageSystemName;
        this.storageSystemDescription = storageSystemDescription;
        this.createdUser = createdUser;
        this.createdTimestamp = createdTimestamp;
        this.updatedUser = updatedUser;
        this.updatedTimestamp = updatedTimestamp;
        this.storageTypeId = storageTypeId;
        this.runningClusterId = runningClusterId;
        this.zoneId = zoneId;
        this.isGimelCompatible = isGimelCompatible;
        this.isReadCompatible = isReadCompatible;
    }

    @Override
    public String toString() {
        return "Storage System ID -> " + this.storageSystemId + " ,Name -> " + this.storageSystemName
                + " ,Description -> " + this.storageSystemDescription + " ,Storage Type ID -> " + this.storageTypeId;
    }

}
