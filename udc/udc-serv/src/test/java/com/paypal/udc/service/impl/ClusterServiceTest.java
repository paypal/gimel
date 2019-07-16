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

package com.paypal.udc.service.impl;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.Arrays;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;
import com.paypal.udc.dao.cluster.ClusterRepository;
import com.paypal.udc.entity.cluster.Cluster;
import com.paypal.udc.util.ClusterUtil;
import com.paypal.udc.util.UserUtil;
import com.paypal.udc.util.enumeration.ActiveEnumeration;
import com.paypal.udc.validator.cluster.ClusterDescValidator;
import com.paypal.udc.validator.cluster.ClusterNameValidator;


@RunWith(SpringRunner.class)
public class ClusterServiceTest {

    @MockBean
    private UserUtil userUtil;

    @MockBean
    private ClusterUtil clusterUtil;

    @Mock
    private ClusterRepository clusterRepository;

    @Mock
    private ClusterDescValidator s2;

    @Mock
    private ClusterNameValidator s1;

    @InjectMocks
    private ClusterService clusterService;

    private long clusterId;
    private String clusterName;
    private Cluster cluster;
    private List<Cluster> clusterList;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        this.clusterId = 1L;
        this.clusterName = "Cluster1";
        this.cluster = new Cluster(this.clusterId, this.clusterName, "Description", "Y", "CrUser",
                "CrTime", "UpdUser", "UpdTime");
        this.clusterList = Arrays.asList(this.cluster);
    }

    @Test
    public void verifyValidGetAllClusters() throws Exception {
        when(this.clusterRepository.findAll()).thenReturn(this.clusterList);

        final List<Cluster> result = this.clusterService.getAllClusters();
        assertEquals(this.clusterList.size(), result.size());

        verify(this.clusterRepository).findAll();
    }

    @Test
    public void verifyValidGetClusterById() throws Exception {
        when(this.clusterUtil.validateCluster(this.clusterId)).thenReturn(this.cluster);

        final Cluster result = this.clusterService.getClusterById(this.clusterId);
        assertEquals(this.cluster, result);

        verify(this.clusterUtil).validateCluster(this.clusterId);
    }

    @Test
    public void verifyValidGetClusterByName() throws Exception {
        when(this.clusterRepository.findByClusterName(this.clusterName)).thenReturn(this.cluster);

        final Cluster result = this.clusterService.getClusterByName(this.clusterName);
        assertEquals(this.cluster, result);

        verify(this.clusterRepository).findByClusterName(this.clusterName);
    }

    @Test
    public void verifyValidAddCluster() throws Exception {
        when(this.clusterRepository.save(this.cluster)).thenReturn(this.cluster);

        final Cluster result = this.clusterService.addCluster(this.cluster);
        assertEquals(this.cluster, result);

        verify(this.clusterRepository).save(this.cluster);
    }

    @Test
    public void verifyValidUpdateCluster() throws Exception {
        when(this.clusterUtil.validateCluster(this.clusterId)).thenReturn(this.cluster);
        when(this.clusterRepository.save(this.cluster)).thenReturn(this.cluster);

        final Cluster result = this.clusterService.updateCluster(this.cluster);
        assertEquals(this.cluster, result);

        verify(this.clusterRepository).save(this.cluster);
    }

    @Test
    public void verifyValidDeActivateCluster() throws Exception {
        when(this.clusterUtil.validateCluster(this.clusterId)).thenReturn(this.cluster);
        when(this.clusterRepository.save(this.cluster)).thenReturn(this.cluster);

        final Cluster result = this.clusterService.deActivateCluster(this.clusterId);
        assertEquals(this.cluster, result);
        assertEquals(ActiveEnumeration.NO.getFlag(), result.getIsActiveYN());

        verify(this.clusterRepository).save(this.cluster);
    }

    @Test
    public void verifyValidReActivateCluster() throws Exception {
        when(this.clusterUtil.validateCluster(this.clusterId)).thenReturn(this.cluster);
        when(this.clusterRepository.save(this.cluster)).thenReturn(this.cluster);

        final Cluster result = this.clusterService.reActivateCluster(this.clusterId);
        assertEquals(this.cluster, result);
        assertEquals(ActiveEnumeration.YES.getFlag(), result.getIsActiveYN());

        verify(this.clusterRepository).save(this.cluster);
    }
}
