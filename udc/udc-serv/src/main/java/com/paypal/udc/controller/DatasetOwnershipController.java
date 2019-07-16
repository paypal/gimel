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

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import javax.mail.MessagingException;
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
import com.google.gson.Gson;
import com.paypal.udc.entity.ownership.DatasetOwnershipMap;
import com.paypal.udc.exception.ValidationError;
import com.paypal.udc.service.IOwnershipDatasetService;
import com.paypal.udc.util.enumeration.UserAttributeEnumeration;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;


@RestController
@RequestMapping("ownership")
@Api(value = "Ownership Service on dataset", description = "Operations pertaining to dataset ownership")
public class DatasetOwnershipController {

    final static Logger logger = LoggerFactory.getLogger(DatasetOwnershipController.class);
    final Gson gson = new Gson();
    private IOwnershipDatasetService ownershipService;
    private HttpServletRequest request;
    private String userType;

    @Autowired
    private DatasetOwnershipController(final IOwnershipDatasetService ownershipService,
            final HttpServletRequest request) {

        this.ownershipService = ownershipService;
        this.request = request;
        this.userType = UserAttributeEnumeration.SUCCESS.getFlag();

    }

    @ApiOperation(value = "View the ownership based on dataset ID", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved Owners for dataset"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("owners/{id}")
	public ResponseEntity<?> getOwnersByDatasetId(@PathVariable("id") final Long id) {
		final List<DatasetOwnershipMap> owners = this.ownershipService.getOwnersByDatasetId(id);
		return new ResponseEntity<List<DatasetOwnershipMap>>(owners, HttpStatus.OK);
	}

    @ApiOperation(value = "View the ownership based on System, Container, Object and Owner", response = DatasetOwnershipMap.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved Owners for dataset"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("owner/{systemName:.+}/{containerName:.+}/{objectName:.+}/{ownerName:.+}")
    public ResponseEntity<?> getOwnersBySystemIdContainerObjectOwner(
            @PathVariable("systemName") final String systemName,
            @PathVariable("containerName") final String containerName,
            @PathVariable("objectName") final String objectName,
            @PathVariable("ownerName") final String ownerName) {
		DatasetOwnershipMap owner;
		try {
			owner = this.ownershipService.getOwnerBySystemContainerObjectAndOwnerName(systemName, containerName,
					objectName, ownerName);
			return new ResponseEntity<DatasetOwnershipMap>(owner, HttpStatus.OK);
		} catch (final ValidationError e) {
			return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
		}
    }

    @ApiOperation(value = "View the ownership based on System, Container, Object", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved Owners for dataset"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("owner/{systemName:.+}/{containerName:.+}/{objectName:.+}")
    public ResponseEntity<?> getOwnersBySystemIdContainerObject(
            @PathVariable("systemName") final String systemName,
            @PathVariable("containerName") final String containerName,
            @PathVariable("objectName") final String objectName) {
		List<DatasetOwnershipMap> owners;
		try {
			owners = this.ownershipService.getOwnersBySystemContainerAndObject(systemName, containerName, objectName);
			return new ResponseEntity<List<DatasetOwnershipMap>>(owners, HttpStatus.OK);
		} catch (final ValidationError e) {
			return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
		}
    }

    @ApiOperation(value = "View a list of available owners", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved list"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("owners")
	public ResponseEntity<?> getAllOwners() {
		final List<DatasetOwnershipMap> list = this.ownershipService.getAllOwners();
		return new ResponseEntity<List<DatasetOwnershipMap>>(list, HttpStatus.OK);
	}

    @ApiOperation(value = "View a list of non notified ownerships", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved list"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
	@GetMapping("newOwners")
	public ResponseEntity<?> getAllUnNotifiedOwnerships() {
		final List<DatasetOwnershipMap> list = this.ownershipService.getAllNewOwners();
		return new ResponseEntity<List<DatasetOwnershipMap>>(list, HttpStatus.OK);
	}

    @ApiOperation(value = "Insert a owner for dataset", response = DatasetOwnershipMap.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully inserted owner"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PostMapping("owner")
    public ResponseEntity<?> addOwner(@RequestBody final DatasetOwnershipMap datasetOwnershipMap) {
        List<DatasetOwnershipMap> insertedOwners;
        try {
            insertedOwners = this.ownershipService.addDatasetOwnershipMap(datasetOwnershipMap);
            return new ResponseEntity<List<DatasetOwnershipMap>>(insertedOwners, HttpStatus.OK);
        }
        catch (final ValidationError e) {
            return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
        }
        catch (final MessagingException e) {
            return new ResponseEntity<String>(this.gson.toJson(e), HttpStatus.BAD_GATEWAY);

        }
        catch (IOException | InterruptedException | ExecutionException e) {
            return new ResponseEntity<String>(this.gson.toJson(e), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @ApiOperation(value = "Update a owner based on Input", response = DatasetOwnershipMap.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully updated Ownership detail"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PutMapping("owner")
    public ResponseEntity<?> updatedDatasetOwnershipMap(@RequestBody final DatasetOwnershipMap datasetOwnershipMap) {
        final DatasetOwnershipMap updatedDatasetOwnershipMap;
        try {
            updatedDatasetOwnershipMap = this.ownershipService.updateDatasetOwnershipMap(datasetOwnershipMap);
            return new ResponseEntity<DatasetOwnershipMap>(updatedDatasetOwnershipMap, HttpStatus.OK);
        }
        catch (final ValidationError e) {
            return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
        }
    }

    @ApiOperation(value = "Update Ownership Notification status", response = DatasetOwnershipMap.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully updated Ownership status"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PutMapping("ownerNotification")
    public ResponseEntity<?> updatedDatasetOwnershipNotificationStatus(
            @RequestBody final DatasetOwnershipMap datasetOwnershipMap) {
        final DatasetOwnershipMap updatedDatasetOwnershipMap;
        try {
            updatedDatasetOwnershipMap = this.ownershipService
                    .updateDatasetOwnershipNotificationStatus(datasetOwnershipMap);
            return new ResponseEntity<DatasetOwnershipMap>(updatedDatasetOwnershipMap, HttpStatus.OK);
        }
        catch (final ValidationError e) {
            return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
        }
    }

}
