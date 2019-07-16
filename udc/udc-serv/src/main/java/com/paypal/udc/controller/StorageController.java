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
import org.springframework.web.util.UriComponentsBuilder;
import com.google.gson.Gson;
import com.paypal.udc.entity.storagecategory.Storage;
import com.paypal.udc.exception.ValidationError;
import com.paypal.udc.service.IStorageService;
import com.paypal.udc.util.enumeration.UserAttributeEnumeration;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;


@RestController
@RequestMapping("storage")
@Api(value = "StorageCategoryService", description = "Operations pertaining to Storage Category")
public class StorageController {

    final static Logger logger = LoggerFactory.getLogger(StorageController.class);

    final Gson gson = new Gson();

    private IStorageService storageService;
    private HttpServletRequest request;
    private String userType;

    @Autowired
    private StorageController(final IStorageService storageService,
            final HttpServletRequest request) {
        this.storageService = storageService;
        this.request = request;
        this.userType = UserAttributeEnumeration.SUCCESS.getFlag();
    }

    @ApiOperation(value = "View the Storage Category based on ID", response = Storage.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved Storage"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("storage/{id}")
	public ResponseEntity<?> getStorageById(@PathVariable("id") final Long id) throws ValidationError {
		final Storage storage = this.storageService.getStorageById(id);
		return new ResponseEntity<Storage>(storage, HttpStatus.OK);
	}

    @ApiOperation(value = "View the Storage Category based on Name", response = Storage.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved Storage"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("storageByName/{name:.+}")
	public ResponseEntity<?> getStorageByTitle(@PathVariable("name") final String name) {
		final Storage storage = this.storageService.getStorageByName(name);
		return new ResponseEntity<Storage>(storage, HttpStatus.OK);

	}

    @ApiOperation(value = "View a list of available Storage Categories", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved list"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
	@GetMapping("storages")
	public ResponseEntity<?> getAllStorages() {

		final List<Storage> list = this.storageService.getAllStorages();
		return new ResponseEntity<List<Storage>>(list, HttpStatus.OK);
	}

    @ApiOperation(value = "Insert an Storage Category", response = String.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully inserted Storage Category"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PostMapping("storage")
    public ResponseEntity<?> addStorage(@RequestBody final Storage storage, final UriComponentsBuilder builder) {
        Storage insertedStorage;
        try {
            try {
                insertedStorage = this.storageService.addStorage(storage);
                return new ResponseEntity<String>(this.gson.toJson(insertedStorage), HttpStatus.OK);
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

    @ApiOperation(value = "Update an Storage Category based on Input", response = String.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully updated Storage"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PutMapping("storage")
    public ResponseEntity<?> updateStorage(@RequestBody final Storage storage) {
        Storage updatedStorage;
        try {
            try {
                updatedStorage = this.storageService.updateStorage(storage);
                return new ResponseEntity<String>(this.gson.toJson(updatedStorage), HttpStatus.OK);
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

    @ApiOperation(value = "Delete an Storage based on ID")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully deleted Storage"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @DeleteMapping("dstorage/{id}")
    public ResponseEntity<?> deleteStorage(@PathVariable("id") final Long id) {
        try {
            Storage storage;
            try {
                storage = this.storageService.deleteStorage(id);
                return new ResponseEntity<String>(this.gson.toJson("Deactivated " + storage.getStorageId()),
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

    @ApiOperation(value = "Enable an Storage based on ID")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully Enabled Storage"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PutMapping("estorage/{id}")
    public ResponseEntity<?> enableStorage(@PathVariable("id") final Long id) {
        try {
            Storage storage;
            try {
                storage = this.storageService.enableStorage(id);
                return new ResponseEntity<String>(this.gson.toJson("Enabled " + storage.getStorageId()), HttpStatus.OK);
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
