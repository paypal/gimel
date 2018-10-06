package com.paypal.udc.entity.dataset;

import java.io.Serializable;


public class ClusterDeploymentStatus implements Serializable {

    private static final long serialVersionUID = 1L;
    public String clusterName;
    public long clusterId;
    public String deploymentStatus;

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

    public String getDeploymentStatus() {
        return this.deploymentStatus;
    }

    public void setDeploymentStatus(final String deploymentStatus) {
        this.deploymentStatus = deploymentStatus;
    }

}
