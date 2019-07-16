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
import javax.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import com.google.gson.Gson;
import com.paypal.udc.entity.zone.Zone;
import com.paypal.udc.exception.ValidationError;
import com.paypal.udc.service.IZoneService;
import com.paypal.udc.util.enumeration.UserAttributeEnumeration;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;


@RestController
@RequestMapping("zone")
@Api(value = "Zone Services", description = "Operations pertaining to Zone Service")
public class ZoneController {

    final static Logger logger = LoggerFactory.getLogger(ZoneController.class);

    final Gson gson = new Gson();
    private IZoneService zoneService;
    private HttpServletRequest request;
    private String userType;

    @Autowired
    private ZoneController(final IZoneService zoneService, final HttpServletRequest request) {
        this.zoneService = zoneService;
        this.request = request;
        this.userType = UserAttributeEnumeration.SUCCESS.getFlag();
    }

    @ApiOperation(value = "View the zone based on ID", response = Zone.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved zone"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found"),
            @ApiResponse(code = 401, message = "The User has no access to this API")
    })
    @GetMapping("zone/{id}")
    public ResponseEntity<?> getZoneById(@PathVariable("id") final Long id) {

		Zone zone;
		try {
			zone = this.zoneService.getZoneById(id);
			return new ResponseEntity<Zone>(zone, HttpStatus.OK);
		}

		catch (final ValidationError e) {
			return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
		}

	}

    @ApiOperation(value = "View the Zone based on Name", response = Zone.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved Zone"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found"),
            @ApiResponse(code = 401, message = "The User has no access to this API")
    })
    @GetMapping("zoneByName/{name:.+}")
    public ResponseEntity<?> getZoneByName(@PathVariable("name") final String name) {

		final Zone zone = this.zoneService.getZoneByName(name);
		return new ResponseEntity<Zone>(zone, HttpStatus.OK);
	}

    @ApiOperation(value = "View a list of available zone", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved list"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found"),
            @ApiResponse(code = 401, message = "The User has no access to this API")

    })
    @GetMapping("zones")
	public ResponseEntity<?> getAllZones() {
		final List<Zone> list = this.zoneService.getAllZones();
		return new ResponseEntity<List<Zone>>(list, HttpStatus.OK);
	}

    @ApiOperation(value = "Insert a Zone", response = Zone.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully inserted Zone"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PostMapping("zone")
    public ResponseEntity<?> addZone(@RequestBody final Zone zone) {
        final Zone insertedZone;
        try {
            insertedZone = this.zoneService.addZone(zone);
            return new ResponseEntity<Zone>(insertedZone, HttpStatus.OK);
        }
        catch (IOException | InterruptedException | ExecutionException e) {
            final ValidationError verror = new ValidationError();
            verror.setErrorCode(HttpStatus.INTERNAL_SERVER_ERROR);
            verror.setErrorDescription(e.getMessage());
            return new ResponseEntity<ValidationError>(verror, verror.getErrorCode());
        }

        catch (final ValidationError e) {
            return new ResponseEntity<ValidationError>(e, e.getErrorCode());
        }
    }

    @ApiOperation(value = "Update an Zone based on Input", response = Zone.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully updated Zone"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PutMapping("zone")
    public ResponseEntity<?> updateZone(@RequestBody final Zone zone) {
        Zone updatedZone;
        try {
            try {
                updatedZone = this.zoneService.updateZone(zone);
                return new ResponseEntity<Zone>(updatedZone, HttpStatus.OK);
            }
            catch (IOException | InterruptedException | ExecutionException e) {
                final ValidationError verror = new ValidationError();
                verror.setErrorCode(HttpStatus.INTERNAL_SERVER_ERROR);
                verror.setErrorDescription(e.getMessage());
                return new ResponseEntity<ValidationError>(verror, verror.getErrorCode());
            }
        }
        catch (final ValidationError e) {
            return new ResponseEntity<ValidationError>(e, e.getErrorCode());
        }
    }

    @ApiOperation(value = "Deactivate a Zone on ID")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully deActivated Zone"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @DeleteMapping("dzone/{id}")
    public ResponseEntity<?> deactivateZone(@PathVariable("id") final Integer id) {
        try {
            Zone zone;
            try {
                zone = this.zoneService.deActivateZone(id);
                return new ResponseEntity<String>(this.gson.toJson("Deactivated " + zone.getZoneId()), HttpStatus.OK);
            }
            catch (IOException | InterruptedException | ExecutionException e) {
                final ValidationError verror = new ValidationError();
                verror.setErrorCode(HttpStatus.INTERNAL_SERVER_ERROR);
                verror.setErrorDescription(e.getMessage());
                return new ResponseEntity<ValidationError>(verror, verror.getErrorCode());
            }
        }
        catch (final ValidationError e) {
            return new ResponseEntity<ValidationError>(e, e.getErrorCode());
        }

    }

    @ApiOperation(value = "Reactivate a Zone on ID")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully reactivated zone"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PutMapping("ezone/{id}")
    public ResponseEntity<?> reactivateZone(@PathVariable("id") final Integer id) {
        try {
            Zone zone;
            try {
                zone = this.zoneService.reActivateZone(id);
                return new ResponseEntity<String>(this.gson.toJson("Reactivated " + zone.getZoneId()), HttpStatus.OK);
            }
            catch (IOException | InterruptedException | ExecutionException e) {
                final ValidationError verror = new ValidationError();
                verror.setErrorCode(HttpStatus.INTERNAL_SERVER_ERROR);
                verror.setErrorDescription(e.getMessage());
                return new ResponseEntity<ValidationError>(verror, verror.getErrorCode());
            }
        }
        catch (final ValidationError e) {
            return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
        }

    }
}
