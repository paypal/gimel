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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import javax.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
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
import com.paypal.udc.entity.dataset.Dataset;
import com.paypal.udc.entity.objectschema.CollectiveObjectSchemaMap;
import com.paypal.udc.entity.objectschema.ObjectAttributeKeyValue;
import com.paypal.udc.entity.objectschema.ObjectSchemaMap;
import com.paypal.udc.exception.ValidationError;
import com.paypal.udc.service.IObjectSchemaMapService;
import com.paypal.udc.util.enumeration.UserAttributeEnumeration;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;


@RestController
@RequestMapping("objectschema")
@Api(value = "Object Schema Load Service", description = "Operations pertaining to Object Schema Map Load")
public class ObjectSchemaMapController {

    final static Logger logger = LoggerFactory.getLogger(ObjectSchemaMapController.class);

    final Gson gson = new Gson();
    private final IObjectSchemaMapService objectSchemaMapService;
    private final HttpServletRequest request;
    private final String userType;

    @Autowired
    public ObjectSchemaMapController(final IObjectSchemaMapService objectSchemaMapService,
            final HttpServletRequest request) {
        this.objectSchemaMapService = objectSchemaMapService;
        this.request = request;
        this.userType = UserAttributeEnumeration.SUCCESS.getFlag();
    }

    @ApiOperation(value = "View the Datasets based on System,Container and Object", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved Dataset"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("schema/{systemName:.+}/{containerName:.+}/{objectName:.+}")
    public ResponseEntity<?> getDatasetBySystemContainerAndObject(
            @PathVariable("systemName") final String systemName,
            @PathVariable("containerName") final String containerName,
            @PathVariable("objectName") final String objectName) {

		List<Dataset> topics;
		try {
			topics = this.objectSchemaMapService.getDatasetBySystemContainerAndObject(systemName, containerName,
					objectName);
			return new ResponseEntity<List<Dataset>>(topics, HttpStatus.OK);
		} catch (final ValidationError e) {
			return new ResponseEntity<List<Dataset>>(new ArrayList<Dataset>(), e.getErrorCode());
		}

	}

    @ApiOperation(value = "View the Datasets based on table", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved Dataset list"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("dataset/{objectName:.+}")
	public ResponseEntity<?> getDatasetsByTable(@PathVariable("objectName") final String objectName) {
		final List<Dataset> topics = this.objectSchemaMapService.getDatasetByObject(objectName);
		return new ResponseEntity<List<Dataset>>(topics, HttpStatus.OK);
	}

    @ApiOperation(value = "View the Object based on System ID,Container and Object", response = ObjectSchemaMap.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved ObjectSchemaMap"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("objectSchema/{systemId}/{containerName:.+}/{objectName:.+}")
	public ResponseEntity<?> getSchema(@PathVariable("systemId") final long systemId,
			@PathVariable("containerName") final String containerName,
			@PathVariable("objectName") final String objectName) {

		final List<CollectiveObjectSchemaMap> schema = this.objectSchemaMapService
				.getSchemaBySystemContainerAndObject(systemId, containerName, objectName);
		return new ResponseEntity<List<CollectiveObjectSchemaMap>>(schema, HttpStatus.OK);
	}

    @ApiOperation(value = "View the Object based on ID", response = ObjectSchemaMap.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved Topic"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("schema/{id}")
	public ResponseEntity<?> getObjectById(@PathVariable("id") final Long id) throws ValidationError {
		final ObjectSchemaMap topic = this.objectSchemaMapService.getDatasetById(id);
		return new ResponseEntity<ObjectSchemaMap>(topic, HttpStatus.OK);
	}

    @ApiOperation(value = "View the Object names based on container Name", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved Table Names"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("objectnames/{containerName:.+}/{storageSystemId}")
	public ResponseEntity<?> getObjectsByContainer(@PathVariable("containerName") final String containerName,
			@PathVariable("storageSystemId") final long storageSystemId) {

		final List<String> objectNames = this.objectSchemaMapService.getDistinctObjectNames(containerName,
				storageSystemId);
		return new ResponseEntity<List<String>>(objectNames, HttpStatus.OK);
	}

    @ApiOperation(value = "View the Containers based on Storage System Id", response = ObjectSchemaMap.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved Topic"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("containers/{storageSystemId}")
	public ResponseEntity<?> getAllContainersByStorageSystemId(
			@PathVariable("storageSystemId") final long storageSystemId) {

		final List<String> containers = this.objectSchemaMapService
				.getDistinctContainerNamesByStorageSystemId(storageSystemId);
		return new ResponseEntity<List<String>>(containers, HttpStatus.OK);
	}

    @ApiOperation(value = "View the Objects based on Storage System Name and Container Name", response = Iterator.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved Objects"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("objects/{storageSystemName:.+}/{containerName:.+}")
	public ResponseEntity<?> getObjectSchemaMapsBySystemAndContainers(
			@PathVariable("storageSystemName") final String storageSystemName,
			@PathVariable("containerName") final String containerName, @RequestParam(defaultValue = "0") final int page,
			@RequestParam(defaultValue = "3") final int size) {

		final Pageable pageable = new PageRequest(page, size);
		final Page<ObjectSchemaMap> objects = this.objectSchemaMapService
				.getObjectsByStorageSystemAndContainer(storageSystemName, containerName, pageable);
		return new ResponseEntity<Page<ObjectSchemaMap>>(objects, HttpStatus.OK);
	}

    @ApiOperation(value = "View the Containers based on Storage System Name", response = Iterator.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved Objects"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("objects/{storageSystemName:.+}")
	public ResponseEntity<?> getObjectSchemaMapsBySystem(
			@PathVariable("storageSystemName") final String storageSystemName,
			@RequestParam(defaultValue = "All") final String objectStr,
			@RequestParam(defaultValue = "0") final int page, @RequestParam(defaultValue = "3") final int size) {

		final Pageable pageable = new PageRequest(page, size);
		final Page<ObjectSchemaMap> objects = this.objectSchemaMapService.getObjectsByStorageSystem(objectStr,
				storageSystemName, pageable);
		return new ResponseEntity<Page<ObjectSchemaMap>>(objects, HttpStatus.OK);

	}

    @ApiOperation(value = "View All the Containers", response = Iterator.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved all Containers"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("containers")
	public ResponseEntity<?> getAllContainers() {

		final List<String> containers = this.objectSchemaMapService.getDistinctContainerNames();
		return new ResponseEntity<List<String>>(containers, HttpStatus.OK);
	}

    @ApiOperation(value = "View All the Containers by System list", response = Iterator.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved all Container by systems"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("containersBySystems/{systemList:.+}")
	public ResponseEntity<?> getAllContainersBySystems(@PathVariable("systemList") final String systemList) {

		final List<String> containers = this.objectSchemaMapService.getDistinctContainerNamesBySystems(systemList);
		return new ResponseEntity<List<String>>(containers, HttpStatus.OK);
	}

    @ApiOperation(value = "View a list of available objects based on Storage System ID", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved page"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("pagedSchemas/{systemId}")
	public ResponseEntity<?> getPagedObjectSchemas(@PathVariable("systemId") final long storageSystemId,
			@RequestParam(defaultValue = "0") final int page, @RequestParam(defaultValue = "3") final int size) {
		final Pageable pageable = new PageRequest(page, size);
		final Page<CollectiveObjectSchemaMap> pagedList = this.objectSchemaMapService
				.getPagedObjectMappings(storageSystemId, pageable);
		return new ResponseEntity<Page<CollectiveObjectSchemaMap>>(pagedList, HttpStatus.OK);
	}

    @ApiOperation(value = "View a list of available Objects with Clusters based on Storage System ID", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved list"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("schemasWithClusters/{systemId}")
	public ResponseEntity<?> getObjectSchemaMapsBySystemIds(@PathVariable("systemId") final long storageSystemId) {

		final List<ObjectSchemaMap> list = this.objectSchemaMapService.getObjectSchemaMapsBySystemIds(storageSystemId);
		return new ResponseEntity<List<ObjectSchemaMap>>(list, HttpStatus.OK);
	}

    @ApiOperation(value = "View a list of available Objects with Clusters based on Storage System ID", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved list"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("customAttribute/{objectId}")
	public ResponseEntity<?> getCustomAttributesByObject(@PathVariable("objectId") final long objectId) {
		final List<ObjectAttributeKeyValue> list = this.objectSchemaMapService.getCustomAttributesByObject(objectId);
		return new ResponseEntity<List<ObjectAttributeKeyValue>>(list, HttpStatus.OK);

	}

    @ApiOperation(value = "Insert Custom Attributes", response = String.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully inserted Object Schema Custom Attribute"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PostMapping("customAttribute")
    public ResponseEntity<String> addObjectSchemaCustomAttribute(@RequestBody final ObjectAttributeKeyValue topic) {
        final ObjectAttributeKeyValue insertedAttributeKeyValue;
        try {
            insertedAttributeKeyValue = this.objectSchemaMapService.addObjectAttributeKeyValue(topic);
            return new ResponseEntity<String>(this.gson.toJson(insertedAttributeKeyValue), HttpStatus.OK);
        }
        catch (IOException | InterruptedException | ExecutionException e) {
            return new ResponseEntity<String>(this.gson.toJson(e), HttpStatus.INTERNAL_SERVER_ERROR);
        }
        catch (final ValidationError e) {
            return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
        }
    }

    @ApiOperation(value = "Update Custom Attribute", response = String.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully updated Object Schema Custom Attribute"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PutMapping("customAttribute")
    public ResponseEntity<String> updateObjectSchemaCustomAttribute(
            @RequestBody final ObjectAttributeKeyValue topics) {
        final ObjectAttributeKeyValue updatedAttributeKeyValue;
        try {
            updatedAttributeKeyValue = this.objectSchemaMapService.updateObjectAttributeKeyValue(topics);
            return new ResponseEntity<String>(this.gson.toJson(updatedAttributeKeyValue), HttpStatus.OK);
        }
        catch (final ValidationError e) {
            return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
        }
        catch (IOException | InterruptedException | ExecutionException e) {
            return new ResponseEntity<String>(this.gson.toJson(e), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @ApiOperation(value = "Insert an Topic", response = String.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully inserted Object Schema"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PostMapping("schema")
    public ResponseEntity<String> addObjectSchema(@RequestBody final ObjectSchemaMap topic,
            final UriComponentsBuilder builder) {
        ObjectSchemaMap insertedTopic;
        try {
            insertedTopic = this.objectSchemaMapService.addObjectSchema(topic);
            return new ResponseEntity<String>(this.gson.toJson(insertedTopic), HttpStatus.OK);
        }
        catch (final ValidationError e) {
            return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
        }
        catch (IOException | InterruptedException | ExecutionException e) {
            return new ResponseEntity<String>(this.gson.toJson(e), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @ApiOperation(value = "View a list of available unregistered Schemas", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved list"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("unregisteredSchemas/{systemId}")
	public ResponseEntity<?> getPagedUnRegisteredObjects(@PathVariable("systemId") final long systemId,
			@RequestParam(defaultValue = "0") final int page, @RequestParam(defaultValue = "3") final int size) {

		final Pageable pageable = new PageRequest(page, size);
		final Page<CollectiveObjectSchemaMap> list = this.objectSchemaMapService.getPagedUnRegisteredObjects(systemId,
				pageable);
		return new ResponseEntity<Page<CollectiveObjectSchemaMap>>(list, HttpStatus.OK);
	}

    @ApiOperation(value = "Update the Schema", response = ObjectSchemaMap.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully updated Object Schema"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PutMapping("schema")
    public ResponseEntity<String> updateSchemaForObject(@RequestBody final ObjectSchemaMap objectSchema,
            final UriComponentsBuilder builder) {
        ObjectSchemaMap updatedTopic;
        try {
            updatedTopic = this.objectSchemaMapService.updateObjectSchemaMap(objectSchema);
            return new ResponseEntity<String>(this.gson.toJson(updatedTopic), HttpStatus.OK);
        }
        catch (final ValidationError e) {
            return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
        }
        catch (IOException | InterruptedException | ExecutionException e) {
            return new ResponseEntity<String>(this.gson.toJson(e), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @ApiOperation(value = "Deactivate the Schema", response = String.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully deactivated Object Schema and Dataset"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PutMapping("deactivate/{objectId}")
    public ResponseEntity<String> deactivateObjectAndDataset(@PathVariable("objectId") final long objectId) {
        try {
            this.objectSchemaMapService.deActivateObjectAndDataset(objectId);
            return new ResponseEntity<String>("Deactivated the Object", HttpStatus.OK);
        }
        catch (final ValidationError e) {
            return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
        }
    }

}
