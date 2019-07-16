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
import com.paypal.udc.entity.integration.description.DatasetColumnDescriptionMap;
import com.paypal.udc.entity.integration.description.DatasetDescriptionMap;
import com.paypal.udc.exception.ValidationError;
import com.paypal.udc.service.IDatasetDescriptionService;
import com.paypal.udc.util.enumeration.UserAttributeEnumeration;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;


@RestController
@RequestMapping("description")
@Api(value = "Description Integration Services", description = "Operations pertaining to Description Integration with UDC")
public class DatasetDescriptionMapController {

    final static Logger logger = LoggerFactory.getLogger(DatasetDescriptionMapController.class);

    final Gson gson = new Gson();

    private IDatasetDescriptionService datasetDescriptionService;
    private HttpServletRequest request;
    private String userType;

    @Autowired
    private DatasetDescriptionMapController(final IDatasetDescriptionService datasetDescriptionService,
            final HttpServletRequest request) {
        this.datasetDescriptionService = datasetDescriptionService;
        this.request = request;
        this.userType = UserAttributeEnumeration.SUCCESS.getFlag();
    }

    @ApiOperation(value = "View the DatasetDescription Schemas based on dataset ID", response = DatasetDescriptionMap.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved DatasetDescriptionMap"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("description/{id}")
    public ResponseEntity<?> getDatasetDescriptionSchemasById(@PathVariable("id") final Long id) {

		final List<DatasetDescriptionMap> schemas = this.datasetDescriptionService.getDatasetSchemasByDatasetId(id);

		return new ResponseEntity<List<DatasetDescriptionMap>>(schemas, HttpStatus.OK);
	}

    @ApiOperation(value = "View the DatasetDescription by system id, objectName and container name", response = DatasetDescriptionMap.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved DatasetDescriptionDatasetMap"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("description/{id}/{containerName:.+}/{objectName:.+}/{providerName:.+}")
    public ResponseEntity<?> getDatasetDescriptionBySystemObjectAndContainerAndProvider(
            @PathVariable("id") final long id, @PathVariable("containerName") final String containerName,
            @PathVariable("objectName") final String objectName,
            @PathVariable("providerName") final String providerName) {

		final DatasetDescriptionMap schemas = this.datasetDescriptionService
				.getDatasetDescriptionMapBySystemContainerAndObjectAndProvider(id, containerName, objectName,
						providerName);
		if (schemas == null) {
			return new ResponseEntity<DatasetDescriptionMap>(new DatasetDescriptionMap(), HttpStatus.OK);
		} else {
			return new ResponseEntity<DatasetDescriptionMap>(schemas, HttpStatus.OK);
		}
	}

    @ApiOperation(value = "View the Dataset Column Description by bodhi dataset map id and column Name", response = DatasetColumnDescriptionMap.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved DatasetColumnDescriptionMap"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("descriptionColumn/{id}/{columnName:.+}")
	public ResponseEntity<?> getDatasetColumnDescriptionByIdAndColumnName(@PathVariable("id") final long id,
			@PathVariable("columnName") final String columnName) {
		final DatasetColumnDescriptionMap schemas = this.datasetDescriptionService
				.getDatasetColumnDescriptionMapByIdAndColumnName(id, columnName);
		return new ResponseEntity<DatasetColumnDescriptionMap>(schemas, HttpStatus.OK);
	}

    @ApiOperation(value = "Insert a DatasetDescription Schema")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully inserted DatasetDescription"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PostMapping("description")
    public ResponseEntity<?> addDatasetDescriptionMap(@RequestBody final DatasetDescriptionMap datasetDesc) {
        try {
            DatasetDescriptionMap outputDesc;
            outputDesc = this.datasetDescriptionService.addDatasetDescriptionMap(datasetDesc);
            return new ResponseEntity<DatasetDescriptionMap>(outputDesc, HttpStatus.OK);
        }
        catch (final ValidationError e) {
            return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
        }
        catch (IOException | InterruptedException | ExecutionException e) {
            return new ResponseEntity<String>(this.gson.toJson(e), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @ApiOperation(value = "Insert a DatasetColumn Description Schema")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully inserted DatasetColumnDescription"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PostMapping("descriptionColumn")
    public ResponseEntity<?> addDatasetColumnDescriptionMap(
            @RequestBody final DatasetColumnDescriptionMap datasetDesc) {
        try {
            final DatasetColumnDescriptionMap outputDesc = this.datasetDescriptionService
                    .addDatasetColumnDecriptionMap(datasetDesc);
            return new ResponseEntity<DatasetColumnDescriptionMap>(outputDesc, HttpStatus.OK);
        }
        catch (final ValidationError e) {
            return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
        }
        catch (IOException | InterruptedException | ExecutionException e) {
            return new ResponseEntity<String>(this.gson.toJson(e), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @ApiOperation(value = "Update Bodhi Schema Definition at object level")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully updated DatasetDescription Schema Definition at object level"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PutMapping("description")
    public ResponseEntity<?> updateBodhiSchemaDatasetMap(@RequestBody final DatasetDescriptionMap input) {
        try {
            final DatasetDescriptionMap datasetDesc = this.datasetDescriptionService.updateDatasetDescriptionMap(input);
            return new ResponseEntity<DatasetDescriptionMap>(datasetDesc, HttpStatus.OK);
        }
        catch (final ValidationError e) {
            return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
        }
        catch (IOException | InterruptedException | ExecutionException e) {
            return new ResponseEntity<String>(this.gson.toJson(e), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @ApiOperation(value = "Update Bodhi Schema Definition at object column level")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully updated DatasetDescription Definition at object's column level"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PutMapping("descriptionColumn")
    public ResponseEntity<?> updateBodhiSchemaDatasetColumnMap(@RequestBody final DatasetColumnDescriptionMap input) {
        try {
            this.datasetDescriptionService.updateDatasetColumnDescriptionMap(input);
            return new ResponseEntity<String>("Successfully updated DatasetDescription column description",
                    HttpStatus.OK);
        }
        catch (final ValidationError e) {
            return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
        }
        catch (IOException | InterruptedException | ExecutionException e) {
            return new ResponseEntity<String>(this.gson.toJson(e), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

}
