package com.paypal.udc.entity.dataset;

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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModelProperty;


@Entity
@Table(name = "pc_storage_dataset_change_log_registered")
public class DatasetChangeLogRegistered implements Serializable {

    private static final long serialVersionUID = 1L;

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @ApiModelProperty(notes = "The database generated change log ID")
    @Column(name = "storage_dataset_change_log_id")
    @NotNull
    private long storageDatasetChangeLogId;

    @ApiModelProperty(notes = "The database generated dataset ID")
    @Column(name = "storage_dataset_id")
    @NotNull
    private long storageDataSetId;

    @ApiModelProperty(notes = "Name of the dataset")
    @Column(name = "storage_dataset_name")
    @NotNull
    private String storageDataSetName;

    @ApiModelProperty(notes = "Name of dataset alias")
    @Column(name = "storage_dataset_alias_name")
    private String storageDataSetAliasName;

    @ApiModelProperty(notes = "Name of the container")
    @Column(name = "storage_container_name")
    private String storageContainerName;

    @ApiModelProperty(notes = "Name of the database")
    @Column(name = "storage_database_name")
    private String storageDatabaseName;

    @ApiModelProperty(notes = "Description on the dataset description")
    @Column(name = "storage_dataset_desc")
    private String storageDataSetDescription;

    @ApiModelProperty(notes = "Storage system ID")
    @Column(name = "storage_system_id")
    @NotNull
    private long storageSystemId;

    @ApiModelProperty(notes = "Storage cluster ID")
    @Column(name = "storage_cluster_id")
    @NotNull
    private long storageClusterId;

    @ApiModelProperty(notes = "Schema of the dataset")
    @Column(name = "storage_dataset_schema")
    private String storageDatasetSchema;

    @ApiModelProperty(notes = "Change Type")
    @Column(name = "storage_dataset_change_type")
    private String storageDatasetChangeType;

    @ApiModelProperty(notes = "Query DDL")
    @Column(name = "storage_dataset_query")
    private String storageDatasetQuery;

    @ApiModelProperty(notes = "User Id")
    @Column(name = "user_id")
    @NotNull
    private long userId;

    @ApiModelProperty(notes = "Deployment Status")
    @Column(name = "storage_deployment_status")
    private String storageDeploymentStatus;

    @ApiModelProperty(notes = "Created User")
    @Column(name = "cre_user")
    @NotNull
    @JsonIgnore
    private String createdUser;

    @ApiModelProperty(notes = "Created Timestamp")
    @Column(name = "cre_ts")
    @NotNull
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

    @Transient
    @JsonIgnore
    private List<CollectiveDatasetAttribute> dataSetAttributes;

    public String getStorageDeploymentStatus() {
        return this.storageDeploymentStatus;
    }

    public void setStorageDeploymentStatus(final String storageDeploymentStatus) {
        this.storageDeploymentStatus = storageDeploymentStatus;
    }

    public String getStorageDataSetAliasName() {
        return this.storageDataSetAliasName;
    }

    public void setStorageDataSetAliasName(final String storageDataSetAliasName) {
        this.storageDataSetAliasName = storageDataSetAliasName;
    }

    public String getStorageContainerName() {
        return this.storageContainerName;
    }

    public void setStorageContainerName(final String storageContainerName) {
        this.storageContainerName = storageContainerName;
    }

    public long getStorageClusterId() {
        return this.storageClusterId;
    }

    public void setStorageClusterId(final long storageClusterId) {
        this.storageClusterId = storageClusterId;
    }

    public String getStorageDatasetQuery() {
        return this.storageDatasetQuery;
    }

    public void setStorageDatasetQuery(final String storageDatasetQuery) {
        this.storageDatasetQuery = storageDatasetQuery;
    }

    @JsonIgnore
    public List<CollectiveDatasetAttribute> getDataSetAttributes() {
        return this.dataSetAttributes;
    }

    @JsonProperty
    public void setDataSetAttributes(final List<CollectiveDatasetAttribute> dataSetAttributes) {
        this.dataSetAttributes = dataSetAttributes;
    }

    public String getStorageDatasetChangeType() {
        return this.storageDatasetChangeType;
    }

    public void setStorageDatasetChangeType(final String storageDatasetChangeType) {
        this.storageDatasetChangeType = storageDatasetChangeType;
    }

    public long getStorageDatasetChangeLogId() {
        return this.storageDatasetChangeLogId;
    }

    public void setStorageDatasetChangeLogId(final long storageDatasetChangeLogId) {
        this.storageDatasetChangeLogId = storageDatasetChangeLogId;
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

    public String getStorageDataSetDescription() {
        return this.storageDataSetDescription;
    }

    public void setStorageDataSetDescription(final String storageDataSetDescription) {
        this.storageDataSetDescription = storageDataSetDescription;
    }

    public long getStorageSystemId() {
        return this.storageSystemId;
    }

    public void setStorageSystemId(final long storageSystemId) {
        this.storageSystemId = storageSystemId;
    }

    public String getStorageDatasetSchema() {
        return this.storageDatasetSchema;
    }

    public void setStorageDatasetSchema(final String storageDatasetSchema) {
        this.storageDatasetSchema = storageDatasetSchema;
    }

    public long getUserId() {
        return this.userId;
    }

    public void setUserId(final long userId) {
        this.userId = userId;
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

    public String getStorageDatabaseName() {
        return this.storageDatabaseName;
    }

    public void setStorageDatabaseName(final String storageDatabaseName) {
        this.storageDatabaseName = storageDatabaseName;
    }

}
