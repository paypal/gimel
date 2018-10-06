package com.paypal.udc.entity.objectschema;

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
import io.swagger.annotations.ApiModelProperty;


@Entity
@Table(name = "pc_object_schema_map")
public class ObjectSchemaMap implements Serializable {

    private static final long serialVersionUID = 1L;

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @ApiModelProperty(notes = "The database generated dataset ID")
    @Column(name = "object_id")
    private long objectId;

    @ApiModelProperty(notes = "Name of the dataset")
    @Column(name = "object_name")
    @Size(min = 1, message = "name need to have atleast 1 character")
    @NotNull
    private String objectName;

    @ApiModelProperty(notes = "Name of the container")
    @Column(name = "container_name")
    private String containerName;

    @ApiModelProperty(notes = "Is it Self Discovered ?")
    @Column(name = "is_self_discovered")
    private String isSelfDiscovered;

    @ApiModelProperty(notes = "Is Registered")
    @Column(name = "is_registered")
    private String isRegistered;

    @ApiModelProperty(notes = "Storage System ID")
    @Column(name = "storage_system_id")
    @NotNull
    private long storageSystemId;

    @ApiModelProperty(notes = "is active flag")
    @Column(name = "is_active_y_n")
    @NotNull
    private String isActiveYN;

    @ApiModelProperty(notes = "created user on store")
    @Column(name = "created_user_on_store")
    @NotNull
    private String createdUserOnStore;

    @ApiModelProperty(notes = "created timestamp on store")
    @Column(name = "created_timestamp_on_store")
    @NotNull
    private String createdTimestampOnStore;

    @ApiModelProperty(notes = "Schema")
    @Column(name = "object_schema", columnDefinition = "json")
    private String objectSchemaInString;

    @Transient
    private List<Schema> objectSchema;

    @ApiModelProperty(notes = "Query")
    @Column(name = "query")
    private String query;

    @ApiModelProperty(notes = "Created User")
    @Column(name = "cre_user")
    private String createdUser;

    @ApiModelProperty(notes = "Created Timestamp")
    @Column(name = "cre_ts")
    private String createdTimestamp;

    @ApiModelProperty(notes = "Updated User")
    @Column(name = "upd_user")
    private String updatedUser;

    @ApiModelProperty(notes = "Updated Timestamp")
    @Column(name = "upd_ts")
    private String updatedTimestamp;

    @Transient
    private List<Long> clusters;

    @Transient
    private List<ObjectAttributeValue> objectAttributes;

    @Transient
    private List<ObjectAttributeValue> pendingAttributes;

    public String getCreatedTimestampOnStore() {
        return this.createdTimestampOnStore == null ? "" : this.createdTimestampOnStore;
    }

    public void setCreatedTimestampOnStore(final String createdTimestampOnStore) {
        this.createdTimestampOnStore = createdTimestampOnStore;
    }

    public String getCreatedUserOnStore() {
        return this.createdUserOnStore == null ? "" : this.createdUserOnStore;
    }

    public void setCreatedUserOnStore(final String createdUserOnStore) {
        this.createdUserOnStore = createdUserOnStore;
    }

    public String getIsRegistered() {
        return this.isRegistered;
    }

    public void setIsRegistered(final String isRegistered) {
        this.isRegistered = isRegistered;
    }

    public String getIsSelfDiscovered() {
        return this.isSelfDiscovered;
    }

    public void setIsSelfDiscovered(final String isSelfDiscovered) {
        this.isSelfDiscovered = isSelfDiscovered;
    }

    public List<ObjectAttributeValue> getPendingAttributes() {
        return this.pendingAttributes;
    }

    public void setPendingAttributes(final List<ObjectAttributeValue> pendingAttributes) {
        this.pendingAttributes = pendingAttributes;
    }

    public String getObjectSchemaInString() {
        return this.objectSchemaInString;
    }

    public void setObjectSchemaInString(final String objectSchemaInString) {
        this.objectSchemaInString = objectSchemaInString;
    }

    public List<Schema> getObjectSchema() {
        return this.objectSchema;
    }

    public void setObjectSchema(final List<Schema> objectSchema) {
        this.objectSchema = objectSchema;
    }

    public String getIsActiveYN() {
        return this.isActiveYN;
    }

    public void setIsActiveYN(final String isActiveYN) {
        this.isActiveYN = isActiveYN;
    }

    public List<ObjectAttributeValue> getObjectAttributes() {
        return this.objectAttributes;
    }

    public void setObjectAttributes(final List<ObjectAttributeValue> objectAttributes) {
        this.objectAttributes = objectAttributes;
    }

    public long getObjectId() {
        return this.objectId;
    }

    public List<Long> getClusters() {
        return this.clusters;
    }

    public void setClusters(final List<Long> clusters) {
        this.clusters = clusters;
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

    // public List<Schema> getObjectSchema() {
    // return this.objectSchema;
    // }
    //
    // public void setObjectSchema(final List<Schema> objectSchema) {
    // this.objectSchema = objectSchema;
    // }

    public String getQuery() {
        return this.query;
    }

    public void setQuery(final String query) {
        this.query = query;
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

    @Override
    public String toString() {

        return "Object ID ->" + this.objectId + " Object Name -> " + this.objectName + " Storage SYstem Id -> "
                + this.storageSystemId + " Container Name -> " + this.containerName;
    }

    public ObjectSchemaMap() {

    }

    public ObjectSchemaMap(final long objectId, final String objectName, final String containerName,
            final String isSelfDiscovered,
            final String isRegistered, final long storageSystemId, final String isActiveYN,
            final String createdUserOnStore,
            final String createdTimestampOnStore, final String objectSchemaInString, final List<Schema> objectSchema,
            final String query,
            final String createdUser, final String createdTimestamp, final String updatedUser,
            final String updatedTimestamp) {
        this.objectId = objectId;
        this.objectName = objectName;
        this.containerName = containerName;
        this.isSelfDiscovered = isSelfDiscovered;
        this.isRegistered = isRegistered;
        this.storageSystemId = storageSystemId;
        this.isActiveYN = isActiveYN;
        this.createdUserOnStore = createdUserOnStore;
        this.createdTimestampOnStore = createdTimestampOnStore;
        this.objectSchemaInString = objectSchemaInString;
        this.objectSchema = objectSchema;
        this.query = query;
        this.createdUser = createdUser;
        this.createdTimestamp = createdTimestamp;
        this.updatedUser = updatedUser;
        this.updatedTimestamp = updatedTimestamp;

    }

}
