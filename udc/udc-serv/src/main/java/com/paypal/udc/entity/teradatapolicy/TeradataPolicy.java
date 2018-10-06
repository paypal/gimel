package com.paypal.udc.entity.teradatapolicy;

import java.io.Serializable;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;
import io.swagger.annotations.ApiModelProperty;


@Entity
@Table(name = "pc_teradata_policy")
public class TeradataPolicy implements Serializable {

    private static final long serialVersionUID = 1L;

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @ApiModelProperty(notes = "The ranger generated auto policy ID")
    @Column(name = "teradata_policy_id")
    @NotNull
    private long teradataPolicyId;

    @ApiModelProperty(notes = "Storage System ID")
    @Column(name = "storage_system_id")
    @NotNull
    private long storageSystemId;

    @ApiModelProperty(notes = "Database name")
    @Column(name = "database_name")
    @NotNull
    private String databaseName;

    @ApiModelProperty(notes = "Role Name")
    @Column(name = "iam_role_name")
    private String iamRoleName;

    @ApiModelProperty(notes = "Created User")
    @Column(name = "cre_user")
    @NotNull
    private String createdUser;

    @ApiModelProperty(notes = "Created TimeStamp")
    @Column(name = "cre_ts")
    @NotNull
    private String createdTimestamp;

    @ApiModelProperty(notes = "Is the rule active ?")
    @Column(name = "is_active_y_n")
    @NotNull
    private String isActiveYN;

    @ApiModelProperty(notes = "Updated User")
    @Column(name = "upd_user")
    @NotNull
    private String updatedUser;

    @ApiModelProperty(notes = "Updated Timestamp")
    @Column(name = "upd_ts")
    @NotNull
    private String updatedTimestamp;

    public long getTeradataPolicyId() {
        return this.teradataPolicyId;
    }

    public void setTeradataPolicyId(final long teradataPolicyId) {
        this.teradataPolicyId = teradataPolicyId;
    }

    public long getStorageSystemId() {
        return this.storageSystemId;
    }

    public void setStorageSystemId(final long storageSystemId) {
        this.storageSystemId = storageSystemId;
    }

    public String getDatabaseName() {
        return this.databaseName;
    }

    public void setDatabaseName(final String databaseName) {
        this.databaseName = databaseName;
    }

    public String getIamRoleName() {
        return this.iamRoleName;
    }

    public void setIamRoleName(final String iamRoleName) {
        this.iamRoleName = iamRoleName;
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

    public String getIsActiveYN() {
        return this.isActiveYN;
    }

    public void setIsActiveYN(final String isActiveYN) {
        this.isActiveYN = isActiveYN;
    }

    public TeradataPolicy() {

    }

    public TeradataPolicy(final long storageSystemId,
            final String databaseName, final String roleName, final String createdUser, final String createdTimestamp,
            final String updatedUser, final String updatedTimestamp, final String isActiveYN) {
        this.storageSystemId = storageSystemId;
        this.databaseName = databaseName;
        this.iamRoleName = roleName;
        this.createdUser = createdUser;
        this.createdTimestamp = createdTimestamp;
        this.updatedUser = updatedUser;
        this.updatedTimestamp = updatedTimestamp;
        this.isActiveYN = isActiveYN;
    }

    public TeradataPolicy(final long teradataPolicyId, final long storageSystemId, final String databaseName,
            final String iamRoleName, final String createdUser, final String createdTimestamp, final String isActiveYN,
            final String updatedUser, final String updatedTimestamp) {
        super();
        this.teradataPolicyId = teradataPolicyId;
        this.storageSystemId = storageSystemId;
        this.databaseName = databaseName;
        this.iamRoleName = iamRoleName;
        this.createdUser = createdUser;
        this.createdTimestamp = createdTimestamp;
        this.isActiveYN = isActiveYN;
        this.updatedUser = updatedUser;
        this.updatedTimestamp = updatedTimestamp;
    }

}
