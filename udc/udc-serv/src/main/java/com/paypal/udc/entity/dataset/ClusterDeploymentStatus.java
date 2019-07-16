/*
 * Copyright 2019 PayPal Inc.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
