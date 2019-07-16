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
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.util.UriComponentsBuilder;
import com.google.gson.Gson;
import com.paypal.udc.entity.teradatapolicy.TeradataPolicy;
import com.paypal.udc.exception.ValidationError;
import com.paypal.udc.service.ITeradataService;
import com.paypal.udc.util.enumeration.UserAttributeEnumeration;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;


@RestController
@RequestMapping("teradata")
@Api(value = "TeradataPolicyService", description = "Operations pertaining to Teradata Policies")
public class TeradataPolicyController {

    final static Logger logger = LoggerFactory.getLogger(TeradataPolicyController.class);

    final Gson gson = new Gson();
    private final ITeradataService teradataService;
    private final HttpServletRequest request;
    private final String userType;

    @Autowired
    private TeradataPolicyController(final ITeradataService teradataService, final HttpServletRequest request) {
        this.teradataService = teradataService;
        this.request = request;
        this.userType = UserAttributeEnumeration.SUCCESS.getFlag();
    }

    @ApiOperation(value = "View a list of available Teradata policies", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved list"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("policies")
	public ResponseEntity<?> getAllPolicies() {

		final List<TeradataPolicy> list = this.teradataService.getAllPolicies();
		return new ResponseEntity<List<TeradataPolicy>>(list, HttpStatus.OK);
	}

    @ApiOperation(value = "View Teradata Policy based on Storage System ID, Mapped Role, Database Name and Role Name", response = TeradataPolicy.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved TeradataPolicy"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("policy/{storageSystemId}/{databaseName}/{roleName}")
	public ResponseEntity<?> getPolicy(@PathVariable("storageSystemId") final long storageSystemId,
			@PathVariable("databaseName") final String databaseName, @PathVariable("roleName") final String roleName) {
		final TeradataPolicy policy = this.teradataService.getPolicyBySystemRuleAndDatabase(storageSystemId,
				databaseName, roleName);
		return new ResponseEntity<TeradataPolicy>(policy, HttpStatus.OK);
	}

    @ApiOperation(value = "View Teradata Policy based on Storage System ID, Mapped Role, Database Name and Role Name", response = TeradataPolicy.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved TeradataPolicy"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("policy/{storageSystemId}/{databaseName:.+}")
	public ResponseEntity<?> getPoliciesByDatabaseAndSystem(@PathVariable("storageSystemId") final long storageSystemId,
			@PathVariable("databaseName") final String databaseName) {

		final List<TeradataPolicy> list = this.teradataService.getPoliciesByDatabaseAndSystem(storageSystemId,
				databaseName);
		return new ResponseEntity<List<TeradataPolicy>>(list, HttpStatus.OK);

	}

    @ApiOperation(value = "Insert a Teradata policy", response = TeradataPolicy.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully inserted TeradataPolicy"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PostMapping("policy")
    public ResponseEntity<?> addPolicy(@RequestBody final TeradataPolicy derivedPolicy,
            final UriComponentsBuilder builder) {
        TeradataPolicy insertedDerivedPolicy;
        try {
            insertedDerivedPolicy = this.teradataService.addPolicy(derivedPolicy);
            return new ResponseEntity<TeradataPolicy>(insertedDerivedPolicy, HttpStatus.OK);
        }
        catch (final ValidationError e) {
            return new ResponseEntity<ValidationError>(e, e.getErrorCode());
        }
    }

    @ApiOperation(value = "Update a Teradata policy based on Input", response = String.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully updated Ranger policy"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PutMapping("policy")
    public ResponseEntity<?> updatePolicy(@RequestBody final TeradataPolicy policy) {
        try {
            this.teradataService.updatePolicy(policy);
            return new ResponseEntity<String>("Updated the Policy with Id " + policy.getTeradataPolicyId(),
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
            this.teradataService.deactivatePolicy(id);
            return new ResponseEntity<String>(this.gson.toJson("Deactivated " + id), HttpStatus.OK);
        }
        catch (final ValidationError e) {
            return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
        }
    }

}
