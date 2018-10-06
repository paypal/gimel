package com.paypal.udc.entity.storagesystem;

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
@Table(name = "pc_storage_system_container")
public class StorageSystemContainer implements Serializable {

    private static final long serialVersionUID = 1L;

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @ApiModelProperty(notes = "The database generated storage system container ID")
    @Column(name = "storage_system_container_id")
    @NotNull
    private long storageSystemContainerId;

    @ApiModelProperty(notes = "The database generated storage system ID")
    @Column(name = "storage_system_id")
    @NotNull
    private long storageSystemId;

    @ApiModelProperty(notes = "Name of the Container")
    @Column(name = "container_name")
    @NotNull
    @Size(min = 1, message = "Name need to have atleast 1 character")
    private String containerName;

    @ApiModelProperty(notes = "Cluster ID fk")
    @Column(name = "cluster_id")
    @NotNull
    private long clusterId;

    @ApiModelProperty(notes = "Is the Storage System Container Type active ?")
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

    public StorageSystemContainer() {
    }

    public long getClusterId() {
        return this.clusterId;
    }

    public void setClusterId(final long clusterId) {
        this.clusterId = clusterId;
    }

    @JsonProperty
    public String getIsActiveYN() {
        return this.isActiveYN;
    }

    @JsonProperty
    public void setIsActiveYN(final String isActiveYN) {
        this.isActiveYN = isActiveYN;
    }

    @JsonProperty
    public long getStorageSystemContainerId() {
        return this.storageSystemContainerId;
    }

    @JsonProperty
    public void setStorageSystemContainerId(final long storageSystemContainerId) {
        this.storageSystemContainerId = storageSystemContainerId;
    }

    @JsonProperty
    public long getStorageSystemId() {
        return this.storageSystemId;
    }

    @JsonProperty
    public void setStorageSystemId(final long storageSystemId) {
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

    @JsonProperty
    public String getContainerName() {
        return this.containerName;
    }

    @JsonProperty
    public void setContainerName(final String containerName) {
        this.containerName = containerName;
    }

    public StorageSystemContainer(final long storageSystemContainerId, final long storageSystemId,
            final String containerName,
            final String createdUser, final String createdTimestamp, final String updatedUser,
            final String updatedTimestamp) {
        this.storageSystemContainerId = storageSystemContainerId;
        this.storageSystemId = storageSystemId;
        this.containerName = containerName;
        this.createdUser = createdUser;
        this.createdTimestamp = createdTimestamp;
        this.updatedUser = updatedUser;
        this.updatedTimestamp = updatedTimestamp;
    }

    @Override
    public String toString() {
        return "Storage System Container ID -> " + this.storageSystemContainerId
                + "Storage System ID -> " + this.storageSystemId
                + " ,Container Name -> " + this.containerName;
    }

}
