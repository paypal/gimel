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

import java.util.List;
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
import com.google.gson.Gson;
import com.paypal.udc.entity.classification.Classification;
import com.paypal.udc.entity.classification.ClassificationDatasetMap;
import com.paypal.udc.exception.ValidationError;
import com.paypal.udc.service.IClassificationDatasetService;
import com.paypal.udc.util.enumeration.UserAttributeEnumeration;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;


@RestController
@RequestMapping("classification")
@Api(value = "Classification Integration Services for datasets", description = "Operations pertaining to classification of datasets")
public class ClassificationMapController {

    final static Logger logger = LoggerFactory.getLogger(ClassificationMapController.class);

    final Gson gson = new Gson();

    private final IClassificationDatasetService classificationDatasetService;
    private final HttpServletRequest request;
    private final String userType;

    @Autowired
    private ClassificationMapController(
            final IClassificationDatasetService classificationDatasetService,
            final HttpServletRequest request) {
        this.classificationDatasetService = classificationDatasetService;
        this.request = request;
        this.userType = UserAttributeEnumeration.SUCCESS.getFlag();
    }

    @ApiOperation(value = "Get all classifications", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved List of classifications for all columns"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("classifications")
	public ResponseEntity<?> getAllClassifications(@RequestParam(defaultValue = "0") final int page,
			@RequestParam(defaultValue = "3") final int size) {

		final Pageable pageable = new PageRequest(page, size);
		Page<Classification> list;
		try {
			list = this.classificationDatasetService.getAllClassifications(pageable);
			return new ResponseEntity<Page<Classification>>(list, HttpStatus.OK);
		} catch (final ValidationError e) {
			return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
		}
	}

    @ApiOperation(value = "Get all classifications by classification Id, zone Id, provider Id, system Id, entity Id", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved list"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("classificationByMiscAttributes")
    public ResponseEntity<?> getClassificationByMiscAttributes(
            @RequestParam("providerIds") final String providerIds,
            @RequestParam("entityIds") final String entityIds,
            @RequestParam("zoneIds") final String zoneIds,
            @RequestParam("systemIds") final String systemIds,
            @RequestParam("classIds") final String classIds,
            @RequestParam(defaultValue = "0") final int page,
            @RequestParam(defaultValue = "3") final int size) {
            final Pageable pageable = new PageRequest(page, size);
            final Page<ClassificationDatasetMap> list = this.classificationDatasetService
                    .getClassificationMapByMiscAttributes(
                            providerIds, entityIds, zoneIds, systemIds, classIds, pageable);
            return new ResponseEntity<Page<ClassificationDatasetMap>>(list, HttpStatus.OK);
    }

    @ApiOperation(value = "Get all classifications by classification Id", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved list"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("classificationById/{classId}")
	public ResponseEntity<?> getClassificationByClass(@PathVariable("classId") final long classificationId,
			@RequestParam(defaultValue = "0") final int page, @RequestParam(defaultValue = "3") final int size) {
		final Pageable pageable = new PageRequest(page, size);
		Page<Classification> list;

		try {
			list = this.classificationDatasetService.getClassificationByClassId(pageable, classificationId);
			return new ResponseEntity<Page<Classification>>(list, HttpStatus.OK);
		} catch (final ValidationError e) {
			return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
		}
	}

    @ApiOperation(value = "View the Classification by objectName and ColumnName", response = Classification.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved DatasetClassificationMap"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("classificationByObjectAndColumn/{objectName:.+}/{columnName:.+}")
	public ResponseEntity<?> getClassificationByObjectAndColumn(@PathVariable("objectName") final String objectName,
			@PathVariable("columnName") final String columnName) {
		final Classification classification = this.classificationDatasetService
				.getClassificationByObjectAndColumn(objectName, columnName);
		return new ResponseEntity<Classification>(classification, HttpStatus.OK);
	}

    @ApiOperation(value = "View the Classification by ObjectName", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved list"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("classificationByObject/{objectName:.+}")
	public ResponseEntity<?> getClassificationByObject(@PathVariable("objectName") final String objectName) {
		final List<Classification> classifications = this.classificationDatasetService
				.getClassificationByObjectName(objectName);
		return new ResponseEntity<List<Classification>>(classifications, HttpStatus.OK);
	}

    @ApiOperation(value = "View the Classification by ColumnName", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved list"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
	@GetMapping("classificationByColumn/{columnName:.+}")
	public ResponseEntity<?> getClassificationByColumn(@PathVariable("columnName") final String columnName) {
		final List<Classification> classifications = this.classificationDatasetService
				.getClassificationByColumnName(columnName);
		return new ResponseEntity<List<Classification>>(classifications, HttpStatus.OK);
	}

    @ApiOperation(value = "Insert a single Classification")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully inserted classification"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PostMapping("/")
    public ResponseEntity<?> addClassification(@RequestBody final Classification input) {
        try {
            final Classification output = this.classificationDatasetService
                    .addClassification(input);
            return new ResponseEntity<Classification>(output, HttpStatus.OK);
        }
        catch (final ValidationError e) {
            return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
        }
    }

    @ApiOperation(value = "Edit a classification based on Input", response = String.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully updated classification"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PutMapping("/")
    public ResponseEntity<?> editClassification(@RequestBody final Classification input) {
        try {
            final Classification output = this.classificationDatasetService
                    .editClassification(input);
            return new ResponseEntity<Classification>(output, HttpStatus.OK);
        }
        catch (final ValidationError e) {
            return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
        }
    }

    @ApiOperation(value = "Insert multiple Classifications")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully inserted classification"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PostMapping("/bulk")
    public ResponseEntity<?> addClassifications(@RequestBody final List<Classification> inputs) {
        try {
            final List<Classification> outputs = this.classificationDatasetService
                    .addClassifications(inputs);
            return new ResponseEntity<List<Classification>>(outputs, HttpStatus.OK);
        }
        catch (final ValidationError e) {
            return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
        }
    }

    @ApiOperation(value = "Deactivate classification")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully deactivated classification"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PutMapping("dclassification/{id}/{user}")
    public ResponseEntity<?> deactivateClassification(@PathVariable("id") final long id,
            @PathVariable("user") final String user) {
        try {
            final Classification classification = this.classificationDatasetService
                    .deActivateClassification(id, user);
            return new ResponseEntity<String>("Successfully deactivated classification", HttpStatus.OK);
        }
        catch (final ValidationError e) {
            return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
        }
    }

    @ApiOperation(value = "Activate classification")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully activated classification"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PutMapping("eclassification/{id}/{user}")
    public ResponseEntity<?> activateClassification(@PathVariable("id") final long id,
            @PathVariable("user") final String user) {
        try {
            final Classification classification = this.classificationDatasetService
                    .reActivateClassification(id, user);
            return new ResponseEntity<String>("Successfully reactivated classification", HttpStatus.OK);
        }
        catch (final ValidationError e) {
            return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
        }
    }
}
