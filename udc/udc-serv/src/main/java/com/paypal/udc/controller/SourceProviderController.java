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
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import com.google.gson.Gson;
import com.paypal.udc.entity.integration.common.SourceProvider;
import com.paypal.udc.exception.ValidationError;
import com.paypal.udc.service.IProviderService;
import com.paypal.udc.util.enumeration.UserAttributeEnumeration;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;


@RestController
@RequestMapping("provider")
@Api(value = "Source Provider Services", description = "Operations pertaining to Source Provider Service")
public class SourceProviderController {

    final static Logger logger = LoggerFactory.getLogger(SourceProviderController.class);

    final Gson gson = new Gson();

    private IProviderService providerService;
    private HttpServletRequest request;
    private String userType;

    @Autowired
    private SourceProviderController(final IProviderService providerService, final HttpServletRequest request) {
        this.providerService = providerService;
        this.request = request;
        this.userType = UserAttributeEnumeration.SUCCESS.getFlag();
    }

    @ApiOperation(value = "View the Provider based on ID", response = SourceProvider.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved SourceProvider"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("provider/{id}")
    public ResponseEntity<?> getProviderById(@PathVariable("id") final Long id) {

		try {
			final SourceProvider sourceProvider = this.providerService.getProviderById(id);
			return new ResponseEntity<SourceProvider>(sourceProvider, HttpStatus.OK);
		} catch (final ValidationError e) {
			return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
		}

    }

    @ApiOperation(value = "View the Provider based on Name", response = SourceProvider.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved SourceProvider"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("providerByName/{name:.+}")
	public ResponseEntity<?> getProviderByName(@PathVariable("name") final String name) {
		final SourceProvider provider = this.providerService.getProviderByName(name);
		return new ResponseEntity<SourceProvider>(provider, HttpStatus.OK);
	}

    @ApiOperation(value = "View a list of available Provider", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved list"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
	@GetMapping("providers")
	public ResponseEntity<?> getAllProviders() {
		final List<SourceProvider> list = this.providerService.getAllProviders();
		return new ResponseEntity<List<SourceProvider>>(list, HttpStatus.OK);
	}

    @ApiOperation(value = "Insert a Provider", response = SourceProvider.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully inserted Source provider"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PostMapping("provider")
    public ResponseEntity<?> addProvider(@RequestBody final SourceProvider provider) {
        SourceProvider insertedProvider;
        try {
            try {
                insertedProvider = this.providerService.addProvider(provider);
                return new ResponseEntity<SourceProvider>(insertedProvider, HttpStatus.OK);
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

    @ApiOperation(value = "Update an Provider based on Input", response = String.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully updated provider"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PutMapping("provider")
    public ResponseEntity<?> updateProvider(@RequestBody final SourceProvider provider) {
        SourceProvider updatedProvider;
        try {
            try {
                updatedProvider = this.providerService.updateProvider(provider);
                return new ResponseEntity<String>(this.gson.toJson(updatedProvider), HttpStatus.OK);
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
