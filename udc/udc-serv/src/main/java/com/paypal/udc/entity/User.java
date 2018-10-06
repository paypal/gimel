package com.paypal.udc.entity;

import java.io.Serializable;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Transient;
import javax.validation.constraints.Size;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModelProperty;


@Entity
@Table(name = "pc_users")
public class User implements Serializable {

    private static final long serialVersionUID = 1L;

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @ApiModelProperty(notes = "The database generated user ID")
    @Column(name = "user_id")
    private long userId;

    @ApiModelProperty(notes = "Name of the user")
    @Column(name = "user_name")
    @Size(min = 1, message = "Name need to have atleast 1 character")
    private String userName;

    @ApiModelProperty(notes = "Full Name of the user")
    @Column(name = "user_full_name")
    private String userFullName;

    @ApiModelProperty(notes = "Is the User active ?")
    @Column(name = "is_active_y_n")
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

    @Transient
    private String roles;

    @Transient
    private String managerName;

    @Transient
    private String organization;

    @Transient
    private String qid;

    public String getQid() {
        return this.qid;
    }

    public void setQid(final String qid) {
        this.qid = qid;
    }

    public String getRoles() {
        return this.roles;
    }

    public void setRoles(final String roles) {
        this.roles = roles;
    }

    public String getManagerName() {
        return this.managerName;
    }

    public void setManagerName(final String managerName) {
        this.managerName = managerName;
    }

    public String getOrganization() {
        return this.organization;
    }

    public void setOrganization(final String organization) {
        this.organization = organization;
    }

    public User() {

    }

    public User(final String userName, final String userFullName,
            final String roles, final String managerName, final String organization, final String qid) {
        super();
        this.userName = userName;
        this.userFullName = userFullName;
        this.roles = roles;
        this.managerName = managerName;
        this.organization = organization;
        this.qid = qid;
    }

    public long getUserId() {
        return this.userId;
    }

    public void setUserId(final long userId) {
        this.userId = userId;
    }

    public String getUserName() {
        return this.userName;
    }

    public void setUserName(final String userName) {
        this.userName = userName;
    }

    public String getUserFullName() {
        return this.userFullName;
    }

    public void setUserFullName(final String userFullName) {
        this.userFullName = userFullName;
    }

    public String getIsActiveYN() {
        return this.isActiveYN;
    }

    public void setIsActiveYN(final String isActiveYN) {
        this.isActiveYN = isActiveYN;
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

}
