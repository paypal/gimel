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

import java.util.List;
import javax.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.util.UriComponentsBuilder;
import com.google.gson.Gson;
import com.paypal.udc.entity.rangerpolicy.DerivedPolicy;
import com.paypal.udc.entity.rangerpolicy.PolicyDiscoveryMetric;
import com.paypal.udc.exception.ValidationError;
import com.paypal.udc.service.IRangerService;
import com.paypal.udc.util.enumeration.UserAttributeEnumeration;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;


@RestController
@RequestMapping("ranger")
@Api(value = "RangerPolicyService", description = "Operations pertaining to Ranger Policies")
public class RangerPolicyController {

    final static Logger logger = LoggerFactory.getLogger(RangerPolicyController.class);

    final Gson gson = new Gson();
    @Autowired
    private IRangerService rangerService;
    private HttpServletRequest request;
    private String userType;

    @Autowired
    private RangerPolicyController(final IRangerService rangerService, final HttpServletRequest request) {
        this.rangerService = rangerService;
        this.request = request;
        this.userType = UserAttributeEnumeration.SUCCESS.getFlag();
    }

    @ApiOperation(value = "View the Ranger policies based on location", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved Ranger policies"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("policiesByLocation")
	public ResponseEntity<?> getPoliciesByLocation(
			@RequestParam(value = "location", required = false) final String location,
			@RequestParam(value = "type", required = true) final String type,
			@RequestParam(value = "cluster", required = true) final long clusterId,
			@RequestParam(value = "table", required = false) final String table,
			@RequestParam(value = "database", required = false) final String database) {

		final List<DerivedPolicy> policies = this.rangerService.getPolicyByPolicyLocations(location, type, clusterId,
				table, database);
		return new ResponseEntity<List<DerivedPolicy>>(policies, HttpStatus.OK);

	}

    @ApiOperation(value = "View the Policy based on Cluster and Policy ID", response = DerivedPolicy.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved Ranger policy"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("policy/{clusterId}/{policyId}")
	public ResponseEntity<?> getPolicyByClusterAndPolicy(@PathVariable("clusterId") final long clusterId,
			@PathVariable("policyId") final int policyId) {

		final DerivedPolicy derivedPolicy = this.rangerService.getPolicyByClusterIdAndPolicyId(clusterId, policyId);
		return new ResponseEntity<DerivedPolicy>(derivedPolicy, HttpStatus.OK);

	}

    @ApiOperation(value = "View a list of available Ranger policies", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved list"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("policiesByCluster/{clusterId}")
	public ResponseEntity<?> getAllPolicies(@PathVariable("clusterId") final long clusterId) {

		final List<DerivedPolicy> list = this.rangerService.getAllPolicies(clusterId);
		return new ResponseEntity<List<DerivedPolicy>>(list, HttpStatus.OK);

	}

    @ApiOperation(value = "Insert a Ranger policy", response = DerivedPolicy.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully inserted Ranger Policy"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PostMapping("policy")
    public ResponseEntity<?> addPolicy(@RequestBody final DerivedPolicy derivedPolicy,
            final UriComponentsBuilder builder) {
        DerivedPolicy insertedDerivedPolicy;
        try {
            insertedDerivedPolicy = this.rangerService.addPolicy(derivedPolicy);
            return new ResponseEntity<DerivedPolicy>(insertedDerivedPolicy, HttpStatus.OK);
        }
        catch (final ValidationError e) {
            return new ResponseEntity<ValidationError>(e, e.getErrorCode());
        }
    }

    @ApiOperation(value = "Insert a Policy Discovery Status", response = PolicyDiscoveryMetric.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully inserted Policy discovery status"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PostMapping("policyDiscoveryStatus")
    public ResponseEntity<?> addPolicyDiscoveryStatus(@RequestBody final PolicyDiscoveryMetric policyStatus,
            final UriComponentsBuilder builder) {
        PolicyDiscoveryMetric insertedPolicyDiscoveryStatus;
        try {
            insertedPolicyDiscoveryStatus = this.rangerService.addPolicyDiscoveryStatus(policyStatus);
            return new ResponseEntity<PolicyDiscoveryMetric>(insertedPolicyDiscoveryStatus, HttpStatus.OK);
        }
        catch (final ValidationError e) {
            return new ResponseEntity<ValidationError>(e, e.getErrorCode());
        }
    }

    @ApiOperation(value = "View a list of available Ranger policies", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved list"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("policyDiscoveryStatus/{discoveryType}")
	public ResponseEntity<?> getRecentPolicyDiscoveryType(@PathVariable("discoveryType") final String discoveryType) {
		final PolicyDiscoveryMetric discoveryMetric = this.rangerService.getRecentPolicyDiscoveryMetric(discoveryType);
		return new ResponseEntity<PolicyDiscoveryMetric>(discoveryMetric, HttpStatus.OK);

	}

    @ApiOperation(value = "Update a Ranger policy based on Input", response = String.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully updated Ranger policy"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PutMapping("policy")
    public ResponseEntity<?> updatePolicy(@RequestBody final DerivedPolicy policy) {
        try {
            this.rangerService.updatePolicy(policy);
            return new ResponseEntity<String>("Updated the Policy with Id " + policy.getDerivedPolicyId(),
                    HttpStatus.OK);
        }
        catch (final ValidationError e) {
            return new ResponseEntity<ValidationError>(e, e.getErrorCode());
        }
    }

    @ApiOperation(value = "Deactivate a policy on ID")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully deactivated Policy"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PutMapping("dpolicy/{id}")
    public ResponseEntity<?> deletePolicy(@PathVariable("id") final Long id) {
        try {
            this.rangerService.deactivatePolicy(id);
            return new ResponseEntity<String>(this.gson.toJson("Deactivated " + id), HttpStatus.OK);
        }
        catch (final ValidationError e) {
            return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
        }
    }

}
