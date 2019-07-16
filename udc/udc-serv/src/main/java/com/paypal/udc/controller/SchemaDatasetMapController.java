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
import com.paypal.udc.entity.integration.schema.SchemaDatasetColumnMap;
import com.paypal.udc.entity.integration.schema.SchemaDatasetMap;
import com.paypal.udc.entity.integration.schema.SchemaDatasetMapInput;
import com.paypal.udc.exception.ValidationError;
import com.paypal.udc.service.ISchemaDatasetService;
import com.paypal.udc.util.enumeration.UserAttributeEnumeration;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;


@RestController
@RequestMapping("schemaIntegration")
@Api(value = "Schema Integration Services", description = "Operations pertaining to Schema Integration with UDC")
public class SchemaDatasetMapController {

    final static Logger logger = LoggerFactory.getLogger(SchemaDatasetMapController.class);

    final Gson gson = new Gson();

    private ISchemaDatasetService schemaDatasetService;
    private HttpServletRequest request;
    private String userType;

    @Autowired
    private SchemaDatasetMapController(final ISchemaDatasetService schemaDatasetService,
            final HttpServletRequest request) {
        this.schemaDatasetService = schemaDatasetService;
        this.request = request;
        this.userType = UserAttributeEnumeration.SUCCESS.getFlag();
    }

    @ApiOperation(value = "View the Schemas based on dataset ID", response = SchemaDatasetMap.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved SchemaDatasetMap"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("schema/{id}")
	public ResponseEntity<?> getSchemasById(@PathVariable("id") final Long id) {

		final List<SchemaDatasetMap> schemas = this.schemaDatasetService.getDatasetSchemasByDatasetId(id);
		return new ResponseEntity<List<SchemaDatasetMap>>(schemas, HttpStatus.OK);
	}

    @ApiOperation(value = "View the Schema by system id, objectName and container name", response = SchemaDatasetMap.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved SchemaDatasetMap"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("schema/{id}/{containerName:.+}/{objectName:.+}/{providerName:.+}")
	public ResponseEntity<?> getSchemasBySystemObjectAndContainer(@PathVariable("id") final long id,
			@PathVariable("containerName") final String containerName,
			@PathVariable("objectName") final String objectName,
			@PathVariable("providerName") final String providerName) {
		SchemaDatasetMap schemas;
		try {
			schemas = this.schemaDatasetService.getDatasetSchemaBySystemObjectAndContainerAndProviderName(id,
					containerName, objectName, providerName);
			return new ResponseEntity<SchemaDatasetMap>(schemas, HttpStatus.OK);

		} catch (final ValidationError e) {
			return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
		}

	}

    @ApiOperation(value = "View the Schema Column by schema dataset map id, column name column code and column datatype", response = SchemaDatasetColumnMap.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved SchemaDatasetColumnMap"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("schemaColumn/{id}/{columnName:.+}/{columnDataType:.+}")
	public ResponseEntity<?> getSchemaColumnBySchemaIdColumnAndDataType(@PathVariable("id") final long id,
			@PathVariable("columnName") final String columnName,
			@PathVariable("columnDataType") final String columnDataType) {
		final SchemaDatasetColumnMap schemas = this.schemaDatasetService
				.getDatasetColumnBySchemaIdColumnNameAndDataType(id, columnName, columnDataType);
		return new ResponseEntity<SchemaDatasetColumnMap>(schemas, HttpStatus.OK);
	}

    @ApiOperation(value = "Insert a Schema via Schema Dump")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully inserted Schema"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PostMapping("schema")
    public ResponseEntity<?> addSchemaDatasetMap(@RequestBody final SchemaDatasetMapInput input) {
        try {
            this.schemaDatasetService.addSchemaDatasetMap(input);
            return new ResponseEntity<String>("Successfully inserted objectSchema descriptions", HttpStatus.OK);
        }
        catch (final ValidationError e) {
            return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
        }
        catch (IOException | InterruptedException | ExecutionException e) {
            return new ResponseEntity<String>(this.gson.toJson(e), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @ApiOperation(value = "Insert a Schema description from UDC")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully inserted Schema Description"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PostMapping("schemaFromUDC")
    public ResponseEntity<?> addSchemaDatasetMapFromUDC(@RequestBody final SchemaDatasetMap input) {
        try {
            SchemaDatasetMap schema;
            schema = this.schemaDatasetService.addDatasetDescriptionMapViaUDC(input);
            return new ResponseEntity<SchemaDatasetMap>(schema, HttpStatus.OK);
        }
        catch (final ValidationError e) {
            return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
        }
        catch (IOException | InterruptedException | ExecutionException e) {
            return new ResponseEntity<String>(this.gson.toJson(e), HttpStatus.INTERNAL_SERVER_ERROR);

        }
    }

    @ApiOperation(value = "Insert a Schema Column description from UDC")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully inserted Schema Column Description"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PostMapping("schemaColumnFromUDC")
    public ResponseEntity<?> addSchemaDatasetColumnMapFromUDC(@RequestBody final SchemaDatasetColumnMap input) {
        try {
            SchemaDatasetColumnMap schema;
            schema = this.schemaDatasetService.addDatasetColumnDescriptionMapViaUDC(input);
            return new ResponseEntity<SchemaDatasetColumnMap>(schema, HttpStatus.OK);
        }
        catch (final ValidationError e) {
            return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
        }
        catch (IOException | InterruptedException | ExecutionException e) {
            return new ResponseEntity<String>(this.gson.toJson(e), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @ApiOperation(value = "Update Schema Definition at object level")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully updated Schema Definition at object level"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PutMapping("schemaDataset")
    public ResponseEntity<?> updateSchemaDatasetMap(@RequestBody final SchemaDatasetMap input) {
        try {
            SchemaDatasetMap datasetMap;
            datasetMap = this.schemaDatasetService.updateSchemaDatasetMap(input);

            return new ResponseEntity<SchemaDatasetMap>(datasetMap, HttpStatus.OK);
        }
        catch (final ValidationError e) {
            return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
        }
        catch (IOException | InterruptedException | ExecutionException e) {
            return new ResponseEntity<String>(this.gson.toJson(e), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @ApiOperation(value = "Update Schema Definition at object column level")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully updated Schema Definition at object's column level"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PutMapping("schemaDatasetColumn")
    public ResponseEntity<?> updateSchemaDatasetColumnMap(@RequestBody final SchemaDatasetColumnMap input) {
        try {
            this.schemaDatasetService.updateSchemaDatasetColumnMap(input);
            return new ResponseEntity<String>("Successfully updated objectSchema column description", HttpStatus.OK);
        }
        catch (final ValidationError e) {
            return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
        }
        catch (IOException | InterruptedException | ExecutionException e) {
            return new ResponseEntity<String>(this.gson.toJson(e), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

}
