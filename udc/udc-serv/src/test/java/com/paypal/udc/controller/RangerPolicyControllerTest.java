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

package com.paypal.udc.controller;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import java.util.Arrays;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;
import com.google.gson.Gson;
import com.paypal.udc.entity.rangerpolicy.DerivedPolicy;
import com.paypal.udc.service.IRangerService;


@RunWith(SpringRunner.class)
@WebMvcTest(RangerPolicyController.class)
public class RangerPolicyControllerTest {

    private MockMvc mockMvc;

    @MockBean
    private IRangerService rangerService;

    @Autowired
    private WebApplicationContext webApplicationContext;

    final Gson gson = new Gson();

    private DerivedPolicy policy;
    private List<DerivedPolicy> policies;
    private String location;
    private String table;
    private String database;
    private Long clusterId;
    private int policyId;
    private Long derivedPolicyId;
    private String jsonPolicy;
    private String type;

    class AnyPolicy implements ArgumentMatcher<DerivedPolicy> {
        @Override
        public boolean matches(final DerivedPolicy derivedPolicy) {
            return derivedPolicy instanceof DerivedPolicy;
        }
    }

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        this.mockMvc = MockMvcBuilders.webAppContextSetup(this.webApplicationContext).build();

        this.policy = new DerivedPolicy();
        this.clusterId = 0L;
        this.policyId = 1;
        this.derivedPolicyId = 2L;
        this.location = "location";
        this.table = "table";
        this.type = "hdfs";
        this.database = "database";
        this.policy.setClusterId(this.clusterId);
        this.policy.setPolicyId(this.policyId);
        this.policy.setDerivedPolicyId(this.derivedPolicyId);
        this.policy.setTypeName(this.type);
        this.policies = Arrays.asList(this.policy);

        this.jsonPolicy = "{ " +
                "\"clusterId\": 0, " +
                "\"createdUser\": \"string\", " +
                "\"derivedPolicyId\": 2, " +
                "\"isActiveYN\": \"string\", " +
                "\"policyId\": 0, " +
                "\"policyItems\": [ " +
                "{ " +
                "\"accessTypes\": \"string\", " +
                "\"createdUser\": \"string\", " +
                "\"derivedPolicyId\": 0, " +
                "\"derivedPolicyUserGroupID\": 0, " +
                "\"groups\": \"string\", " +
                "\"isActiveYN\": \"string\", " +
                "\"updatedUser\": \"string\", " +
                "\"users\": \"string\" " +
                "} " +
                "], " +
                "\"policyLocations\": \"string\", " +
                "\"policyName\": \"string\", " +
                "\"typeName\": \"string\", " +
                "\"updatedUser\": \"string\" " +
                "} ";
    }

    @Test
    public void verifyValidGetPoliciesByLocation() throws Exception {
        when(this.rangerService.getPolicyByPolicyLocations(this.location, this.type, this.clusterId, this.table,
                this.database))
                        .thenReturn(this.policies);

        this.mockMvc.perform(
                get("/ranger/policiesByLocation?location=location&type=hdfs&cluster=0&table=table&database=database")
                        .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(this.policies.size())));

        verify(this.rangerService).getPolicyByPolicyLocations(this.location, this.type, this.clusterId, this.table,
                this.database);
    }

    @Test
    public void verifyValidGetPolicyByClusterAndPolicy() throws Exception {
        when(this.rangerService.getPolicyByClusterIdAndPolicyId(this.clusterId, this.policyId)).thenReturn(this.policy);

        this.mockMvc.perform(get("/ranger/policy/{clusterId}/{policyId}", this.clusterId, this.policyId)
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.clusterId").value(this.clusterId));

        verify(this.rangerService).getPolicyByClusterIdAndPolicyId(this.clusterId, this.policyId);
    }

    @Test
    public void verifyValidGetAllPolicies() throws Exception {
        when(this.rangerService.getAllPolicies(this.clusterId)).thenReturn(this.policies);

        this.mockMvc.perform(get("/ranger/policiesByCluster/{clusterId}", this.clusterId, this.policyId)
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(this.policies.size())));

        verify(this.rangerService).getAllPolicies(this.clusterId);
    }

    @Test
    public void verifyValidAddPolicy() throws Exception {
        when(this.rangerService.addPolicy(argThat(new AnyPolicy())))
                .thenReturn(this.policy);

        this.mockMvc.perform(post("/ranger/policy")
                .contentType(MediaType.APPLICATION_JSON)
                .content(this.jsonPolicy)
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.clusterId").exists())
                .andExpect(jsonPath("$.clusterId").value(this.clusterId));

        verify(this.rangerService).addPolicy(argThat(new AnyPolicy()));
    }

    @Test
    public void verifyValidUpdatePolicy() throws Exception {
        final String expectedResult = "Updated the Policy with Id " + this.policy.getDerivedPolicyId();

        doNothing().when(this.rangerService).updatePolicy(argThat(new AnyPolicy()));

        this.mockMvc.perform(put("/ranger/policy")
                .contentType(MediaType.APPLICATION_JSON)
                .content(this.jsonPolicy)
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString(expectedResult)));

        verify(this.rangerService).updatePolicy(argThat(new AnyPolicy()));
    }

    @Test
    public void verifyValidDeletePolicy() throws Exception {
        final String expectedResult = "Deactivated " + this.policyId;

        doNothing().when(this.rangerService).deactivatePolicy(this.policyId);

        this.mockMvc.perform(put("/ranger/dpolicy/{id}", this.policyId)
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString(expectedResult)));

        verify(this.rangerService).deactivatePolicy(this.policyId);
    }

}
