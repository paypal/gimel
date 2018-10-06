package com.paypal.udc.entity.rangerpolicy;

import java.io.Serializable;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;
import com.fasterxml.jackson.annotation.JsonIgnore;
import io.swagger.annotations.ApiModelProperty;


@Entity
@Table(name = "pc_ranger_policy_user_group")
public class DerivedPolicyItem implements Serializable {

    private static final long serialVersionUID = 1L;

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @ApiModelProperty(notes = "The database generated dataset ID")
    @Column(name = "derived_policy_user_group_id")
    private long derivedPolicyUserGroupID;

    @ApiModelProperty(notes = "The ranger generated auto policy ID foreign key")
    @Column(name = "derived_policy_id")
    @NotNull
    private long derivedPolicyId;

    @ApiModelProperty(notes = "Policy access types")
    @Column(name = "access_types")
    private String accessTypes;

    @ApiModelProperty(notes = "users")
    @Column(name = "users")
    private String users;

    @ApiModelProperty(notes = "groups")
    @Column(name = "groups")
    private String groups;

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

    public DerivedPolicyItem() {

    }

    public DerivedPolicyItem(final long derivedPolicyUserGroupID, final long derivedPolicyId, final String accessTypes,
            final String users, final String groups, final String isActiveYN, final String createdUser,
            final String createdTimestamp, final String updatedUser, final String updatedTimestamp) {
        this.derivedPolicyUserGroupID = derivedPolicyUserGroupID;
        this.derivedPolicyId = derivedPolicyId;
        this.accessTypes = accessTypes;
        this.users = users;
        this.groups = groups;
        this.isActiveYN = isActiveYN;
        this.createdUser = createdUser;
        this.createdTimestamp = createdTimestamp;
        this.updatedUser = updatedUser;
        this.updatedTimestamp = updatedTimestamp;
    }

    public long getDerivedPolicyUserGroupID() {
        return this.derivedPolicyUserGroupID;
    }

    public void setDerivedPolicyUserGroupID(final long derivedPolicyUserGroupID) {
        this.derivedPolicyUserGroupID = derivedPolicyUserGroupID;
    }

    public long getDerivedPolicyId() {
        return this.derivedPolicyId;
    }

    public void setDerivedPolicyId(final long derivedPolicyId) {
        this.derivedPolicyId = derivedPolicyId;
    }

    public String getIsActiveYN() {
        return this.isActiveYN;
    }

    public void setIsActiveYN(final String isActiveYN) {
        this.isActiveYN = isActiveYN;
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

    public String getAccessTypes() {
        return this.accessTypes;
    }

    public void setAccessTypes(final String accessTypes) {
        this.accessTypes = accessTypes;
    }

    public String getUsers() {
        return this.users;
    }

    public void setUsers(final String users) {
        this.users = users;
    }

    public String getGroups() {
        return this.groups;
    }

    public void setGroups(final String groups) {
        this.groups = groups;
    }

}
