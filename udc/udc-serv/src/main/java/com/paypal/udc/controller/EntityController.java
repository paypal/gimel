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
import com.paypal.udc.entity.entity.Entity;
import com.paypal.udc.exception.ValidationError;
import com.paypal.udc.service.IEntityService;
import com.paypal.udc.util.enumeration.UserAttributeEnumeration;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;


@RestController
@RequestMapping("entity")
@Api(value = "Entity Services", description = "Operations pertaining to Entity Service")
public class EntityController {

    final static Logger logger = LoggerFactory.getLogger(EntityController.class);

    final Gson gson = new Gson();
    private IEntityService entityService;
    private HttpServletRequest request;
    private String userType;

    @Autowired
    private EntityController(final IEntityService entityService, final HttpServletRequest request) {
        this.entityService = entityService;
        this.request = request;
        this.userType = UserAttributeEnumeration.SUCCESS.getFlag();
    }

    @ApiOperation(value = "View the entity based on ID", response = Entity.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved entity"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found"),
            @ApiResponse(code = 401, message = "The User has no access to this API")
    })
    @GetMapping("entity/{id}")
	public ResponseEntity<?> getEntityById(@PathVariable("id") final Long id) {
		try {
			final Entity entity = this.entityService.getEntityById(id);
			return new ResponseEntity<Entity>(entity, HttpStatus.OK);
		} catch (final ValidationError e) {
			return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
		}
	}

    @ApiOperation(value = "View the Entity based on Name", response = Entity.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved Entity"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found"),
            @ApiResponse(code = 401, message = "The User has no access to this API")
    })
    @GetMapping("entityByName/{name:.+}")
	public ResponseEntity<?> getEntityByName(@PathVariable("name") final String name) {
		final Entity entity = this.entityService.getEntityByName(name);
		return new ResponseEntity<Entity>(entity, HttpStatus.OK);
	}

    @ApiOperation(value = "View a list of available entity", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved list"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found"),
            @ApiResponse(code = 401, message = "The User has no access to this API")

    })
    @GetMapping("entities")
	public ResponseEntity<?> getAllentities() {
		final List<Entity> list = this.entityService.getAllEntities();
		return new ResponseEntity<List<Entity>>(list, HttpStatus.OK);
	}

    @ApiOperation(value = "Insert a Entity", response = Entity.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully inserted Entity"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PostMapping("entity")
    public ResponseEntity<?> addEntity(@RequestBody final Entity entity) {
        final Entity insertedEntity;
        try {
            try {
                insertedEntity = this.entityService.addEntity(entity);
                return new ResponseEntity<Entity>(insertedEntity, HttpStatus.OK);
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

    @ApiOperation(value = "Update a Entity based on Input", response = Entity.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully updated Entity"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PutMapping("entity")
    public ResponseEntity<?> updateEntity(@RequestBody final Entity entity) {
        Entity updatedEntity;
        try {
            try {
                updatedEntity = this.entityService.updateEntity(entity);
                return new ResponseEntity<Entity>(updatedEntity, HttpStatus.OK);
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

    @ApiOperation(value = "Deactivate a Entity on ID")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully deActivated Entity"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @DeleteMapping("dentity/{id}")
    public ResponseEntity<?> deactivateEntity(@PathVariable("id") final Integer id) {
        try {
            Entity entity;
            try {
                entity = this.entityService.deActivateEntity(id);
                return new ResponseEntity<String>(this.gson.toJson("Deactivated " + entity.getEntityId()),
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

    @ApiOperation(value = "Reactivate a Entity on ID")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully reactivated entity"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PutMapping("eentity/{id}")
    public ResponseEntity<?> reactivateEntity(@PathVariable("id") final Integer id) {
        try {
            Entity entity;
            try {
                entity = this.entityService.reActivateEntity(id);
                return new ResponseEntity<String>(this.gson.toJson("Reactivated " + entity.getEntityId()),
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
