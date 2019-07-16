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
import com.paypal.udc.entity.storagetype.CollectiveStorageTypeAttributeKey;
import com.paypal.udc.entity.storagetype.StorageType;
import com.paypal.udc.entity.storagetype.StorageTypeAttributeKey;
import com.paypal.udc.exception.ValidationError;
import com.paypal.udc.service.IStorageTypeService;
import com.paypal.udc.util.enumeration.UserAttributeEnumeration;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;


@RestController
@RequestMapping("storageType")
@Api(value = "StorageTypeService", description = "Operations pertaining to Storage Type")
public class StorageTypeController {

    final static Logger logger = LoggerFactory.getLogger(StorageTypeController.class);

    final Gson gson = new Gson();
    private IStorageTypeService storageTypeService;
    private HttpServletRequest request;
    private String userType;

    @Autowired
    private StorageTypeController(final IStorageTypeService storageTypeService,
            final HttpServletRequest request) {
        this.storageTypeService = storageTypeService;
        this.request = request;
        this.userType = UserAttributeEnumeration.SUCCESS.getFlag();
    }

    @ApiOperation(value = "View the Storage Type based on ID", response = StorageType.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved Storage Type"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("storageType/{id}")
	public ResponseEntity<?> getStorageTypeById(@PathVariable("id") final Long id) throws ValidationError {
		final StorageType storageType = this.storageTypeService.getStorageTypeById(id);
		return new ResponseEntity<StorageType>(storageType, HttpStatus.OK);

	}

    @ApiOperation(value = "Get Storage Attribute Keys for a Storage Type at System Level", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved Storage Attribute Keys"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("storageAttributeKey/{id}/{isStorageSystemLevel}")
	public ResponseEntity<?> getStorageAttributeKeysById(@PathVariable("id") final Long id,
			@PathVariable("isStorageSystemLevel") final String isStorageSystemLevel) {

		final List<StorageTypeAttributeKey> storageAttributeKeys = this.storageTypeService.getStorageAttributeKeys(id,
				isStorageSystemLevel);
		return new ResponseEntity<List<StorageTypeAttributeKey>>(storageAttributeKeys, HttpStatus.OK);
	}

    @ApiOperation(value = "Get All Storage Attribute Keys for a Storage Type", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved Storage Attribute Keys"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
	@GetMapping("storageAttributeKey/{id}")
	public ResponseEntity<?> getAllStorageAttributeKeysById(@PathVariable("id") final Long id) {
		final List<StorageTypeAttributeKey> storageAttributeKeys = this.storageTypeService
				.getAllStorageAttributeKeys(id);
		return new ResponseEntity<List<StorageTypeAttributeKey>>(storageAttributeKeys, HttpStatus.OK);
	}

    @ApiOperation(value = "View the Storage Type based on Storage ID", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved Storage"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("storageTypeByStorageId/{storageId}")
	public ResponseEntity<?> getStorageTypeByStorage(@PathVariable("storageId") final long storageId) {
		final List<StorageType> storageTypes = this.storageTypeService.getStorageTypeByStorageCategory(storageId);
		return new ResponseEntity<List<StorageType>>(storageTypes, HttpStatus.OK);
	}

    @ApiOperation(value = "View the Storage Type based on Storage Name", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved Storage"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("storageTypeByStorageName/{storageName:.+}")
	public ResponseEntity<?> getStorageTypeByStorageName(@PathVariable("storageName") final String storageName) {
		final List<StorageType> storageTypes = this.storageTypeService.getStorageTypeByStorageCategoryName(storageName);
		return new ResponseEntity<List<StorageType>>(storageTypes, HttpStatus.OK);
	}

    @ApiOperation(value = "View a list of available Storage Types", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved list"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("storageTypes")
	public ResponseEntity<?> getAllStorageTypes() {
		final List<StorageType> list = this.storageTypeService.getAllStorageTypes();
		return new ResponseEntity<List<StorageType>>(list, HttpStatus.OK);

	}

    @ApiOperation(value = "Insert an Storage Type", response = String.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully inserted Storage Type"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PostMapping("storageType")
    public ResponseEntity<?> addStorageType(@RequestBody final StorageType storageType) {
        StorageType insertedStorageType;
        try {
            try {
                insertedStorageType = this.storageTypeService.addStorageType(storageType);
                return new ResponseEntity<String>(this.gson.toJson(insertedStorageType), HttpStatus.CREATED);
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

    @ApiOperation(value = "Insert an Storage Type Attribute", response = StorageTypeAttributeKey.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully inserted Storage Type Attribute"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PostMapping("storageTypeAttribute")
    public ResponseEntity<?> addStorageTypeAttribute(@RequestBody final CollectiveStorageTypeAttributeKey stak) {
        StorageTypeAttributeKey insertedStak;
        try {
            insertedStak = this.storageTypeService.insertStorageTypeAttributeKey(stak);
            return new ResponseEntity<StorageTypeAttributeKey>(insertedStak, HttpStatus.CREATED);
        }
        catch (final ValidationError e) {
            return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
        }
    }

    @ApiOperation(value = "Update an Storage Type based on Input", response = String.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully updated Storage Type"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PutMapping("storageType")
    public ResponseEntity<?> updateStorageType(@RequestBody final StorageType storageType) {
        StorageType updatedStorageType;
        try {
            try {
                updatedStorageType = this.storageTypeService.updateStorageType(storageType);
                return new ResponseEntity<String>(this.gson.toJson(updatedStorageType), HttpStatus.OK);
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

    @ApiOperation(value = "Delete an Storage Type based on ID")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully deleted Storage Type"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @DeleteMapping("dstorageType/{id}")
    public ResponseEntity<?> deleteStorageType(@PathVariable("id") final long id) {
        try {
            StorageType storageType;
            try {
                storageType = this.storageTypeService.deleteStorageType(id);
                return new ResponseEntity<String>(this.gson.toJson("Deactivated " + storageType.getStorageTypeId()),
                        HttpStatus.OK);
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

    @ApiOperation(value = "Enable an Storage Type based on ID")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully Enabled Storage Type"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PutMapping("estorageType/{id}")
    public ResponseEntity<String> enableStorage(@PathVariable("id") final Long id) {
        try {
            final StorageType storageType = this.storageTypeService.enableStorageType(id);
            return new ResponseEntity<String>(this.gson.toJson("Enabled " + storageType.getStorageTypeId()),
                    HttpStatus.OK);
        }
        catch (final ValidationError e) {
            return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
        }

    }
}
