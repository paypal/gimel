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
import java.text.ParseException;
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
import com.paypal.udc.entity.storagesystem.StorageSystem;
import com.paypal.udc.entity.storagesystem.StorageSystemAttributeValue;
import com.paypal.udc.entity.storagesystem.StorageSystemDiscovery;
import com.paypal.udc.exception.ValidationError;
import com.paypal.udc.service.IStorageSystemService;
import com.paypal.udc.util.enumeration.UserAttributeEnumeration;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;


@RestController
@RequestMapping("storageSystem")
@Api(value = "StorageSystemService", description = "Operations pertaining to Storage System")
public class StorageSystemController {

    final static Logger logger = LoggerFactory.getLogger(StorageSystemController.class);

    final Gson gson = new Gson();

    private final IStorageSystemService storageSystemService;
    private final HttpServletRequest request;
    private final String userType;

    @Autowired
    private StorageSystemController(final IStorageSystemService storageSystemService, final HttpServletRequest request) {
        this.storageSystemService = storageSystemService;
        this.request = request;
        this.userType = UserAttributeEnumeration.SUCCESS.getFlag();
    }

    @ApiOperation(value = "View the Storage System based on System ID", response = StorageSystem.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved Storage System"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
	@GetMapping("storageSystem/{id}")
	public ResponseEntity<?> getStorageSystemById(@PathVariable("id") final Long id) throws ValidationError {
		final StorageSystem storageSystem = this.storageSystemService.getStorageSystemById(id);
		return new ResponseEntity<StorageSystem>(storageSystem, HttpStatus.OK);

	}

    @ApiOperation(value = "View the Storage System Attributes based on System ID", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved Storage System Attributes"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("storageSystemAttributes/{id}")
	public ResponseEntity<?> getStorageAttributesById(@PathVariable("id") final Long id) {
		final List<StorageSystemAttributeValue> storageAttributes = this.storageSystemService
				.getStorageSystemAttributes(id);
		return new ResponseEntity<List<StorageSystemAttributeValue>>(storageAttributes, HttpStatus.OK);
	}

    @ApiOperation(value = "View the Storage System Attributes based on Storage System Name", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved Storage System Attributes"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("storageSystemAttributesByName/{storageSystemName:.+}")
	public ResponseEntity<?> getStorageAttributesByName(@PathVariable("storageSystemName") final String systemName) {
		List<StorageSystemAttributeValue> storageAttributes;
		try {
			storageAttributes = this.storageSystemService.getAttributeValuesByName(systemName);
			return new ResponseEntity<List<StorageSystemAttributeValue>>(storageAttributes, HttpStatus.OK);
		} catch (final ValidationError e) {
			return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
		}
	}

    @ApiOperation(value = "View the Storage System based on Storage Type ID", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved Storage"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("storageSystemByType/{storageTypeId}")
	public ResponseEntity<?> getStorageSystemByType(@PathVariable("storageTypeId") final long storageTypeId) {
		final List<StorageSystem> storageSystems = this.storageSystemService
				.getStorageSystemByStorageType(storageTypeId);
		return new ResponseEntity<List<StorageSystem>>(storageSystems, HttpStatus.OK);

	}

    @ApiOperation(value = "View the Storage System based on Storage Type Name", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved Storage Systems"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("storageSystemByTypeName/{storageTypeName:.+}")
	public ResponseEntity<?> getStorageSystemByTypeName(@PathVariable("storageTypeName") final String typeName) {

		List<StorageSystem> storageSystems;
		try {
			storageSystems = this.storageSystemService.getStorageSystemByType(typeName);
			return new ResponseEntity<List<StorageSystem>>(storageSystems, HttpStatus.OK);
		} catch (final ValidationError e) {
			return new ResponseEntity<ValidationError>(e, e.getErrorCode());
		}

	}

    @ApiOperation(value = "View the Storage System based on Zone Name", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved Storage Systems by zone"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("storageSystemByZoneName/{zoneName:.+}/{typeName:.+}")
	public ResponseEntity<?> getStorageSystemByZoneName(@PathVariable("zoneName") final String zoneName,
			@PathVariable("typeName") final String typeName) {
		List<StorageSystem> storageSystems;
		try {
			storageSystems = this.storageSystemService.getStorageSystemByZoneAndType(zoneName, typeName);
			return new ResponseEntity<List<StorageSystem>>(storageSystems, HttpStatus.OK);
		} catch (final ValidationError e) {
			return new ResponseEntity<ValidationError>(e, e.getErrorCode());
		}
	}

    @ApiOperation(value = "View a list of available Storage Systems", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved list"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
	@GetMapping("storageSystems")
	public ResponseEntity<?> getAllStorageSystems() {
		final List<StorageSystem> list = this.storageSystemService.getAllStorageSystems();
		return new ResponseEntity<List<StorageSystem>>(list, HttpStatus.OK);
	}

    @ApiOperation(value = "Insert an Storage System", response = StorageSystem.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully inserted Storage System"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PostMapping("storageSystem")
    public ResponseEntity<?> addStorageSystem(@RequestBody final StorageSystem storageSystem) {
        StorageSystem insertedStorageSystem;
        try {
            try {
                insertedStorageSystem = this.storageSystemService.addStorageSystem(storageSystem);
                return new ResponseEntity<StorageSystem>(insertedStorageSystem, HttpStatus.OK);
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

    @ApiOperation(value = "Insert an Storage System Discovery status", response = StorageSystemDiscovery.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully inserted Storage System Discovery"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PostMapping("storageSystemDiscovery")
    public ResponseEntity<?> addStorageSystemDiscovery(
            @RequestBody final StorageSystemDiscovery storageSystemDiscovery) {
        final StorageSystemDiscovery insertedStorageSystemDiscovery;
        try {
            insertedStorageSystemDiscovery = this.storageSystemService
                    .addStorageSystemDiscovery(storageSystemDiscovery);
            return new ResponseEntity<StorageSystemDiscovery>(insertedStorageSystemDiscovery, HttpStatus.OK);
        }
        catch (final ValidationError e) {
            return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
        }
        catch (final ParseException e) {
            return new ResponseEntity<String>(this.gson.toJson(e), HttpStatus.BAD_REQUEST);
        }
    }

    @ApiOperation(value = "Get the run details of a give datastore", response = StorageSystemDiscovery.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully inserted Storage System Discovery"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
	@GetMapping("storageSystemDiscovery/{storageSystemList}")
	public ResponseEntity<?> getStorageSystemDiscovery(
			@PathVariable("storageSystemList") final String storageSystemList) {
		final List<StorageSystemDiscovery> storageSystemDiscovery = this.storageSystemService
				.getDiscoveryStatusForStorageSystemId(storageSystemList);
		return new ResponseEntity<List<StorageSystemDiscovery>>(storageSystemDiscovery, HttpStatus.OK);
	}

    @ApiOperation(value = "Update an Storage System based on Input", response = StorageSystem.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully updated Storage System"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PutMapping("storageSystem")
    public ResponseEntity<?> updateStorageSystem(@RequestBody final StorageSystem storageSystem) {
        StorageSystem updatedStorageSystem;
        try {
            try {
                updatedStorageSystem = this.storageSystemService.updateStorageSystem(storageSystem);
                return new ResponseEntity<StorageSystem>(updatedStorageSystem, HttpStatus.OK);
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

    @ApiOperation(value = "Delete an Storage System based on ID")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully deleted Storage System"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @DeleteMapping("dstorageSystem/{id}")
    public ResponseEntity<?> deleteStorageSystem(@PathVariable("id") final long id) {
        try {
            StorageSystem storageSystem;
            try {
                storageSystem = this.storageSystemService.deleteStorageSystem(id);
                return new ResponseEntity<String>(this.gson.toJson("Deactivated " + storageSystem.getStorageSystemId()),
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

    @ApiOperation(value = "Re-Enable a Storage System based on ID")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully enabled Storage System"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PutMapping("estorageSystem/{id}")
    public ResponseEntity<?> enableStorageSystem(@PathVariable("id") final long id) {
        try {
            StorageSystem storageSystem;
            try {
                storageSystem = this.storageSystemService.enableStorageSystem(id);
                return new ResponseEntity<String>(this.gson.toJson("Reactivated " + storageSystem.getStorageSystemId()),
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

}
