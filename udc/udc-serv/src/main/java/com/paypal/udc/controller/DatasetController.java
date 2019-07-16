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
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import javax.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import com.google.gson.Gson;
import com.paypal.udc.entity.dataset.CumulativeDataset;
import com.paypal.udc.entity.dataset.Dataset;
import com.paypal.udc.entity.dataset.DatasetChangeLog;
import com.paypal.udc.entity.dataset.DatasetWithAttributes;
import com.paypal.udc.entity.dataset.ElasticSearchDataset;
import com.paypal.udc.exception.ValidationError;
import com.paypal.udc.service.IDatasetService;
import com.paypal.udc.util.enumeration.UserAttributeEnumeration;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;


@RestController
@RequestMapping("dataSet")
@Api(value = "DataSetService", description = "Operations pertaining to Dataset")
public class DatasetController {

    final static Logger logger = LoggerFactory.getLogger(DatasetController.class);

    final Gson gson = new Gson();
    private final IDatasetService dataSetService;
    private final HttpServletRequest request;
    private final String userType;

    @Autowired
    private DatasetController(final IDatasetService dataSetService,
            final HttpServletRequest request) {
        this.dataSetService = dataSetService;
        this.request = request;
        this.userType = UserAttributeEnumeration.SUCCESS.getFlag();
    }

    @ApiOperation(value = "View the Total Datasets Count", response = Long.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved Dataset count"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("totalUniqueCount")
    public ResponseEntity<?> getDatasetCount() {
		final long datasetCount = this.dataSetService.getDatasetCount();
		return new ResponseEntity<Long>(datasetCount, HttpStatus.OK);
	}

    @ApiOperation(value = "View a timelineList ", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved list"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("timelineList/")
	public ResponseEntity<?> getTimelineDimension() {
		final Map<String, List<String>> list = this.dataSetService.getTimelineDimensions();
		return new ResponseEntity<>(list, HttpStatus.OK);
	}

    @ApiOperation(value = "View the basic dataset details based on ID", response = Dataset.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved Dataset"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("dataSet/{id}")
	public ResponseEntity<?> getDataSetById(@PathVariable("id") final Long id) throws ValidationError {
		final Dataset dataSet = this.dataSetService.getDataSetById(id);
		return new ResponseEntity<Dataset>(dataSet, HttpStatus.OK);
	}

    @ApiOperation(value = "View the Dataset with system, schema and object attributes given a dataset ID", response = DatasetWithAttributes.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved DatasetWithAttributes"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("dataSetPending/{id}")
	public ResponseEntity<?> getPendingDataSetById(@PathVariable("id") final Long id) {
		DatasetWithAttributes dataSet;
		try {
			dataSet = this.dataSetService.getPendingDataset(id);
			return new ResponseEntity<DatasetWithAttributes>(dataSet, HttpStatus.OK);
		} catch (final ValidationError e) {
			return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
		}
	}

    @ApiOperation(value = "View the Dataset with system, schema and object attributes based on dataset name", response = DatasetWithAttributes.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved Dataset"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("dataSetByName/{dataSetName:.+}")
    public ResponseEntity<?> getDataSetByName(@PathVariable("dataSetName") final String dataSetName) {    
		try {
			final DatasetWithAttributes dataSet = this.dataSetService.getDataSetByName(dataSetName);
			return new ResponseEntity<DatasetWithAttributes>(dataSet, HttpStatus.OK);
		} catch (final ValidationError e) {
			return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
		}
    }

    @ApiOperation(value = "View the Change Log based on dataset ID", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved Dataset"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
	@GetMapping("changeLogs/{datasetId}")
	public ResponseEntity<?> getChangeLogsByDataSetId(@PathVariable("datasetId") final Long datasetId) {
		final List<DatasetChangeLog> changeLogs = this.dataSetService.getChangeLogsByDataSetId(datasetId);
		return new ResponseEntity<List<DatasetChangeLog>>(changeLogs, HttpStatus.OK);
	}

    @ApiOperation(value = "View the Change Log based on dataset ID and change column type", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved Dataset"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("changeLogs/{datasetId}/{changeType}")
	public ResponseEntity<?> getChangeLogsByDataSetIdAndChangeColumnType(
			@PathVariable("datasetId") final Long datasetId, @PathVariable("changeType") final String changeType) {
		final List<DatasetChangeLog> changeLogs = this.dataSetService
				.getChangeLogsByDataSetIdAndChangeColumnType(datasetId, changeType);
		return new ResponseEntity<List<DatasetChangeLog>>(changeLogs, HttpStatus.OK);
	}

    @ApiOperation(value = "View a list of available datasets based on the prefix ", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved list"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("dataSets/")
	public ResponseEntity<?> getAllDataSets(@RequestParam(value = "prefix") final String dataSetSubString) {
		final List<CumulativeDataset> list = this.dataSetService.getAllDatasets(dataSetSubString);
		return new ResponseEntity<List<CumulativeDataset>>(list, HttpStatus.OK);
	}

    @ApiOperation(value = "View a list of available datasets based on the prefix ", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved list"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("dataSets/v2/")
	public ResponseEntity<?> getAllDataSetsFromElasticSearchBySystemIds(
			@RequestParam(value = "prefix", defaultValue = "All") final String dataSetSubString,
			@RequestParam(defaultValue = "0") final int page, @RequestParam(defaultValue = "3") final int size,
			@RequestParam(defaultValue = "DATASET") final String searchType,
			@RequestParam(defaultValue = "All") final String container,
			@RequestParam(defaultValue = "All") final String storageSystemList) {

		final Pageable pageable = new PageRequest(page, size);
		Page<ElasticSearchDataset> list;
		try {
			list = this.dataSetService.getAllDatasetsFromESByType(dataSetSubString, pageable, searchType,
					storageSystemList, container);
			return new ResponseEntity<Page<ElasticSearchDataset>>(list, HttpStatus.OK);

		} catch (final ValidationError e) {
			return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
		}
	}

    @ApiOperation(value = "View a list of available detailed datasets based on the prefix ", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved list"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("detailedDataSets/")
    public ResponseEntity<?> getAllDetailedDataSets(
			@RequestParam(value = "prefixString") final String dataSetSubString) {
		final List<CumulativeDataset> list = this.dataSetService.getAllDetailedDatasets(dataSetSubString);
		return new ResponseEntity<List<CumulativeDataset>>(list, HttpStatus.OK);
    }

    @ApiOperation(value = "View a list of Pending datasets based on storage system Name and type name", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved list"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("dataSetsBySystemPending/{storageTypeName:.+}/{storageSystemName:.+}")
    public ResponseEntity<?> getAllPendingDataSetsBySystemName(
            @PathVariable("storageSystemName") final String storageSystemName,
            @PathVariable("storageTypeName") final String storageTypeName,
            @RequestParam(defaultValue = "All") final String datasetStr,
            @RequestParam(defaultValue = "0") final int page,
            @RequestParam(defaultValue = "3") final int size) {
		final Pageable pageable = new PageRequest(page, size);
		Page<Dataset> list;
		try {
			list = this.dataSetService.getPendingDatasets(datasetStr, storageTypeName, storageSystemName, pageable);
			return new ResponseEntity<Page<Dataset>>(list, HttpStatus.OK);
		} catch (final ValidationError e) {
			return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
		}
    }

    @ApiOperation(value = "View a list of available datasets based on storage system Name and type name", response = Iterable.class)
    @ApiResponses(value = {

            @ApiResponse(code = 200, message = "Successfully retrieved list"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("dataSetsBySystem/{storageTypeName:.+}/{storageSystemName:.+}")
    public ResponseEntity<?> getAllDataSetsBySystemName(
            @PathVariable("storageTypeName") final String storageTypeName,
            @PathVariable("storageSystemName") final String storageSystemName,
            @RequestParam(defaultValue = "All") final String datasetStr,
            @RequestParam(defaultValue = "All") final String zoneName,
            @RequestParam(defaultValue = "0") final int page,
            @RequestParam(defaultValue = "3") final int size) {
		final Pageable pageable = new PageRequest(page, size);

		Page<Dataset> list;
		try {
			list = this.dataSetService.getAllDatasetsByTypeAndSystem(datasetStr, storageTypeName, storageSystemName,
					zoneName, pageable);
			return new ResponseEntity<Page<Dataset>>(list, HttpStatus.OK);
		} catch (final ValidationError e) {
			return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
		}
    }

    @ApiOperation(value = "View a list of deactivated datasets based on storage system Name and type name", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved list"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("dataSetsBySystemDeleted/{storageTypeName:.+}/{storageSystemName:.+}")
    public ResponseEntity<?> getAllDeactivatedDataSetsBySystemName(
            @PathVariable("storageTypeName") final String storageTypeName,
            @PathVariable("storageSystemName") final String storageSystemName,
            @RequestParam(defaultValue = "All") final String datasetStr,
            @RequestParam(defaultValue = "0") final int page,
            @RequestParam(defaultValue = "3") final int size) throws ValidationError {
		final Pageable pageable = new PageRequest(page, size);
		final Page<Dataset> list = this.dataSetService.getAllDeletedDatasetsByTypeAndSystem(datasetStr, storageTypeName,
				storageSystemName, pageable);
		return new ResponseEntity<Page<Dataset>>(list, HttpStatus.OK);
    }

    @ApiOperation(value = "Insert a dataset via UDC", response = String.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully inserted dataSet"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PostMapping("addDataSet")
    public ResponseEntity<?> addDatasetViaUDC(@RequestBody final Dataset dataSet) {
        try {
            Dataset insertedDataset;
            insertedDataset = this.dataSetService.addDataset(dataSet);
            return new ResponseEntity<String>(this.gson.toJson(insertedDataset.getStorageDataSetId()), HttpStatus.OK);
        }
        catch (final ValidationError e) {
            return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
        }
        catch (IOException | InterruptedException | ExecutionException e) {
            return new ResponseEntity<String>(this.gson.toJson(e), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @ApiOperation(value = "Insert a dataset via Discovery Service", response = String.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully inserted dataSet"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PostMapping("dataSet")
    public ResponseEntity<?> addDataSet(@RequestBody final Dataset dataSet) {
        try {
            Dataset insertedDataset;
            insertedDataset = this.dataSetService.addDataset(dataSet);
            return new ResponseEntity<String>(this.gson.toJson(insertedDataset.getStorageDataSetId()), HttpStatus.OK);
        }
        catch (final ValidationError e) {
            return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
        }
        catch (IOException | InterruptedException | ExecutionException e) {
            return new ResponseEntity<String>(this.gson.toJson(e), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @ApiOperation(value = "Update a dataset based on Input", response = String.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully updated dataset"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PutMapping("dataSet")
    public ResponseEntity<String> updateDataSet(@RequestBody final Dataset dataSet) {
        Dataset updatedDataSet;
        try {
            updatedDataSet = this.dataSetService.updateDataSet(dataSet);
            return new ResponseEntity<String>(this.gson.toJson(updatedDataSet.getStorageDataSetId()), HttpStatus.OK);
        }
        catch (final ValidationError e) {
            return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
        }
        catch (IOException | InterruptedException | ExecutionException e) {
            return new ResponseEntity<String>(this.gson.toJson(e), HttpStatus.INTERNAL_SERVER_ERROR);

        }
    }

    @ApiOperation(value = "Update a dataset based on Newly added object attributes", response = String.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully updated dataset with "),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PutMapping("dataSetWithObjectAttributes")
    public ResponseEntity<?> updateDataSetWithAttributes(@RequestBody final DatasetWithAttributes dataSet) {
        try {
            this.dataSetService.updateDatasetWithPendingAttributes(dataSet);
            return new ResponseEntity<String>(this.gson.toJson("Updated Dataset " + dataSet.getStorageDataSetId()),
                    HttpStatus.OK);
        }
        catch (final ValidationError e) {
            return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
        }
    }

    @ApiOperation(value = "Delete a dataset based on ID")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully deleted dataset"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @DeleteMapping("dataSet/{id}")
    public ResponseEntity<String> deleteDataSet(@PathVariable("id") final long id) {
        try {
            Dataset dataset;
            dataset = this.dataSetService.deleteDataSet(id);
            return new ResponseEntity<String>(this.gson.toJson("Deleted " + dataset.getStorageDataSetId()),
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
