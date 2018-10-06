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
@Table(name = "pc_storage_clusters")
public class Cluster implements Serializable {

    private static final long serialVersionUID = 1L;

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @ApiModelProperty(notes = "The database generated storage cluster ID")
    @Column(name = "cluster_id")
    @NotNull
    private long clusterId;

    @ApiModelProperty(notes = "Name of the Storage Cluster")
    @Column(name = "cluster_name")
    @Size(min = 1, message = "name need to have atleast 1 character")
    @NotNull
    private String clusterName;

    @ApiModelProperty(notes = "Description on the Storage Cluster")
    @Column(name = "cluster_description")
    private String clusterDescription;

    @ApiModelProperty(notes = "Livy End point for the Storage Cluster")
    @Column(name = "livy_end_point")
    private String livyEndPoint;

    @ApiModelProperty(notes = "Livy Running port")
    @Column(name = "livy_port")
    private long livyPort;

    @ApiModelProperty(notes = "Is the Cluster active ?")
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

    public Cluster() {

    }

    public long getClusterId() {
        return this.clusterId;
    }

    public void setClusterId(final long clusterId) {
        this.clusterId = clusterId;
    }

    public String getClusterName() {
        return this.clusterName;
    }

    public void setClusterName(final String clusterName) {
        this.clusterName = clusterName;
    }

    public String getClusterDescription() {
        return this.clusterDescription;
    }

    public void setClusterDescription(final String clusterDescription) {
        this.clusterDescription = clusterDescription;
    }

    public String getLivyEndPoint() {
        return this.livyEndPoint;
    }

    public void setLivyEndPoint(final String livyEndPoint) {
        this.livyEndPoint = livyEndPoint;
    }

    public long getLivyPort() {
        return this.livyPort;
    }

    public void setLivyPort(final long livyPort) {
        this.livyPort = livyPort;
    }

    public Cluster(final long clusterId, final String clusterName, final String clusterDescription,
            final String livyEndPoint, final long livyPort, final String isActiveYN, final String createdUser,
            final String createdTimestamp, final String updatedUser, final String updatedTimestamp) {
        super();
        this.clusterId = clusterId;
        this.clusterName = clusterName;
        this.clusterDescription = clusterDescription;
        this.livyEndPoint = livyEndPoint;
        this.livyPort = livyPort;
        this.isActiveYN = isActiveYN;
        this.createdUser = createdUser;
        this.createdTimestamp = createdTimestamp;
        this.updatedUser = updatedUser;
        this.updatedTimestamp = updatedTimestamp;
    }

}
