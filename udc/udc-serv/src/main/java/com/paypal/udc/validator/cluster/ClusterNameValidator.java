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

package com.paypal.udc.validator.cluster;

import org.springframework.stereotype.Component;
import com.paypal.udc.entity.cluster.Cluster;
import com.paypal.udc.exception.ValidationError;


@Component
public class ClusterNameValidator implements ClusterValidator {

    private ClusterValidator chain;

    @Override
    public void setNextChain(final ClusterValidator nextChain) {
        this.chain = nextChain;
    }

    @Override
    public void validate(final Cluster cluster, final Cluster updatedCluster) throws ValidationError {
        if (cluster.getClusterName() != null && cluster.getClusterName().length() >= 0) {
            updatedCluster.setClusterName(cluster.getClusterName());
        }
        this.chain.validate(cluster, updatedCluster);
    }

}
