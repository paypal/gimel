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
import javax.validation.constraints.Size;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.paypal.udc.entity.rangerpolicy.DerivedPolicy;
import com.paypal.udc.entity.teradatapolicy.TeradataPolicy;
import io.swagger.annotations.ApiModelProperty;


@Entity
@Table(name = "pc_storage_dataset")
public class Dataset implements Serializable {

    private static final long serialVersionUID = 1L;

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @ApiModelProperty(notes = "The database generated dataset ID")
    @Column(name = "storage_dataset_id")
    @NotNull
    private long storageDataSetId;

    @ApiModelProperty(notes = "Name of the dataset")
    @Column(name = "storage_dataset_name")
    @NotNull
    @Size(min = 1, message = "Name need to have atleast 1 character")
    private String storageDataSetName;

    @ApiModelProperty(notes = "Name of dataset alias")
    @Column(name = "storage_dataset_alias_name")
    private String storageDataSetAliasName;

    @ApiModelProperty(notes = "Name of the database")
    @Column(name = "storage_database_name")
    private String storageDatabaseName;

    @ApiModelProperty(notes = "Description on the dataset description")
    @Column(name = "storage_dataset_desc")
    private String storageDataSetDescription;

    @ApiModelProperty(notes = "Is the Dataset active ?")
    @Column(name = "is_active_y_n")
    private String isActiveYN;

    @ApiModelProperty(notes = "User Id")
    @Column(name = "user_id")
    @NotNull
    private long userId;

    @ApiModelProperty(notes = "Created User")
    @Column(name = "cre_user")
    @NotNull
    private String createdUser;

    @ApiModelProperty(notes = "Created Timestamp")
    @Column(name = "cre_ts")
    @NotNull
    private String createdTimestamp;

    @ApiModelProperty(notes = "Updated User")
    @Column(name = "upd_user")
    private String updatedUser;

    @ApiModelProperty(notes = "Updated Timestamp")
    @Column(name = "upd_ts")
    private String updatedTimestamp;

    @ApiModelProperty(notes = "Is the dataset auto registered")
    @Column(name = "is_auto_registered")
    @NotNull
    private String isAutoRegistered;

    @ApiModelProperty(notes = "Object Schema Map Id")
    @Column(name = "object_schema_map_id")
    private long objectSchemaMapId;

    @Transient
    @JsonIgnore
    private String storageContainerName;

    @Transient
    @JsonIgnore
    private String objectName;

    @Transient
    @JsonIgnore
    private List<Long> clusters;

    @Transient
    private String storageSystemName;

    @Transient
    private List<ClusterDeploymentStatus> clusterNames;

    @ApiModelProperty(notes = "Storage System ID")
    @Column(name = "storage_system_id")
    private long storageSystemId;

    @Transient
    private boolean isAttributesPresent;

    @Transient
    private String isGimelCompatible;

    @Transient
    private String zoneName;

    @Transient
    private String isAccessControlled;

    @Transient
    private List<TeradataPolicy> teradataPolicies;

    @Transient
    private List<DerivedPolicy> derivedPolicies;

    @Transient
    private String isReadCompatible;

    public String getIsReadCompatible() {
        return this.isReadCompatible;
    }

    public void setIsReadCompatible(final String isReadCompatible) {
        this.isReadCompatible = isReadCompatible;
    }

    public List<DerivedPolicy> getDerivedPolicies() {
        return this.derivedPolicies;
    }

    public void setDerivedPolicies(final List<DerivedPolicy> derivedPolicies) {
        this.derivedPolicies = derivedPolicies;
    }

    public List<TeradataPolicy> getTeradataPolicies() {
        return this.teradataPolicies;
    }

    public void setTeradataPolicies(final List<TeradataPolicy> teradataPolicies) {
        this.teradataPolicies = teradataPolicies;
    }

    public String getIsAccessControlled() {
        return this.isAccessControlled;
    }

    public void setIsAccessControlled(final String isAccessControlled) {
        this.isAccessControlled = isAccessControlled;
    }

    public String getZoneName() {
        return this.zoneName;
    }

    public void setZoneName(final String zoneName) {
        this.zoneName = zoneName;
    }

    public String getIsGimelCompatible() {
        return this.isGimelCompatible;
    }

    public void setIsGimelCompatible(final String isGimelCompatible) {
        this.isGimelCompatible = isGimelCompatible;
    }

    public boolean isAttributesPresent() {
        return this.isAttributesPresent;
    }

    public void setAttributesPresent(final boolean isAttributesPresent) {
        this.isAttributesPresent = isAttributesPresent;
    }

    public List<ClusterDeploymentStatus> getClusterNames() {
        return this.clusterNames;
    }

    public void setClusterNames(final List<ClusterDeploymentStatus> clusterNames) {
        this.clusterNames = clusterNames;
    }

    @JsonIgnore
    public String getObjectName() {
        return this.objectName;
    }

    @JsonProperty
    public void setObjectName(final String objectName) {
        this.objectName = objectName;
    }

    public long getObjectSchemaMapId() {
        return this.objectSchemaMapId;
    }

    public void setObjectSchemaMapId(final long objectSchemaMapId) {
        this.objectSchemaMapId = objectSchemaMapId;
    }

    public String getStorageSystemName() {
        return this.storageSystemName;
    }

    public void setStorageSystemName(final String storageSystemName) {
        this.storageSystemName = storageSystemName;
    }

    public String getStorageDataSetAliasName() {
        return this.storageDataSetAliasName;
    }

    public void setStorageDataSetAliasName(final String storageDataSetAliasName) {
        this.storageDataSetAliasName = storageDataSetAliasName;
    }

    @JsonIgnore
    public List<Long> getClusters() {
        return this.clusters;
    }

    @JsonProperty
    public void setClusters(final List<Long> clusters) {
        this.clusters = clusters;
    }

    public String getIsActiveYN() {
        return this.isActiveYN;
    }

    public void setIsActiveYN(final String isActiveYN) {
        this.isActiveYN = isActiveYN;
    }

    // public List<DatasetAttributeValue> getDataSetAttributeValues() {
    // return this.dataSetAttributeValues;
    // }
    //
    // public void setDataSetAttributeValues(final List<DatasetAttributeValue> dataSetAttributeValues) {
    // this.dataSetAttributeValues = dataSetAttributeValues;
    // }

    public long getUserId() {
        return this.userId;
    }

    public void setUserId(final long userId) {
        this.userId = userId;
    }

    public long getStorageSystemId() {
        return this.storageSystemId;
    }

    public void setStorageSystemId(final long storageSystemId) {
        this.storageSystemId = storageSystemId;
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
    public String getStorageContainerName() {
        return this.storageContainerName;
    }

    @JsonProperty
    public void setStorageContainerName(final String storageContainerName) {
        this.storageContainerName = storageContainerName;
    }

    public String getStorageDatabaseName() {
        return this.storageDatabaseName;
    }

    public void setStorageDatabaseName(final String storageDatabaseName) {
        this.storageDatabaseName = storageDatabaseName;
    }

    public String getIsAutoRegistered() {
        return this.isAutoRegistered;
    }

    public void setIsAutoRegistered(final String isAutoRegistered) {
        this.isAutoRegistered = isAutoRegistered;
    }

    public Dataset() {

    }

    public Dataset(final String storageDataSetName, final long storageSystemId, final String storageDataSetAliasName,
            final String storageContainerName, final String storageDatabaseName, final String storageDataSetDescription,
            final String isActiveYN, final String createdUser, final String createdTimestamp, final String updatedUser,
            final String updatedTimestamp, final long userId, final String isAutoRegistered) {
        this.storageDataSetName = storageDataSetName;
        this.storageDataSetAliasName = storageDataSetAliasName;
        this.storageContainerName = storageContainerName;
        this.storageDatabaseName = storageDatabaseName;
        this.storageDataSetDescription = storageDataSetDescription;
        this.isActiveYN = isActiveYN;
        this.createdUser = createdUser;
        this.createdTimestamp = createdTimestamp;
        this.updatedUser = updatedUser;
        this.updatedTimestamp = updatedTimestamp;
        this.userId = userId;
        this.isAutoRegistered = isAutoRegistered;
        this.storageSystemId = storageSystemId;

    }

    public Dataset(final String storageDataSetName, final String storageDataSetAliasName,
            final String storageContainerName, final String storageDatabaseName, final String storageDataSetDescription,
            final String isActiveYN, final String createdUser, final String createdTimestamp, final String updatedUser,
            final String updatedTimestamp, final long userId, final String isAutoRegistered,
            final long objectSchemaMapId, final long storageSystemId) {
        this.storageDataSetName = storageDataSetName;
        this.storageDataSetAliasName = storageDataSetAliasName;
        this.storageContainerName = storageContainerName;
        this.storageDatabaseName = storageDatabaseName;
        this.storageDataSetDescription = storageDataSetDescription;
        this.isActiveYN = isActiveYN;
        this.createdUser = createdUser;
        this.createdTimestamp = createdTimestamp;
        this.updatedUser = updatedUser;
        this.updatedTimestamp = updatedTimestamp;
        this.userId = userId;
        this.isAutoRegistered = isAutoRegistered;
        this.objectSchemaMapId = objectSchemaMapId;
        this.storageSystemId = storageSystemId;
    }

}
