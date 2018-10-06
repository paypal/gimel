package com.paypal.udc.controller;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
import com.paypal.udc.cache.DatasetCache;
import com.paypal.udc.entity.dataset.CumulativeDataset;
import com.paypal.udc.entity.dataset.Dataset;
import com.paypal.udc.entity.dataset.DatasetChangeLogRegistered;
import com.paypal.udc.entity.dataset.DatasetWithAttributes;
import com.paypal.udc.exception.ValidationError;
import com.paypal.udc.service.IDatasetService;
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
    @Value("${application.livy.env}")
    private String isProd;
    @Autowired
    private IDatasetService dataSetService;
    @Autowired
    private DatasetCache dataSetCache;

    @ApiOperation(value = "View the Total Datasets Count", response = String.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved Dataset count"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("totalUniqueCount")
    public ResponseEntity<Long> getDatasetCount() {
        final long datasetCount = this.dataSetService.getDatasetCount();
        return new ResponseEntity<Long>(datasetCount, HttpStatus.OK);
    }

    @ApiOperation(value = "View the basic dataset details based on ID", response = Dataset.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved Dataset"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("dataSet/{id}")
    public ResponseEntity<Dataset> getDataSetById(@PathVariable("id") final Long id) {
        final Dataset dataSet = this.dataSetCache.getDataSet(id);
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
        }
        catch (final ValidationError e) {
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
        }
        catch (final ValidationError e) {
            return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
        }

    }

    @ApiOperation(value = "View the Change Log based on cluster ID", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved Change Log"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("changeLogs/{clusterId}")
    public ResponseEntity<List<DatasetChangeLogRegistered>> getLogs(@PathVariable("clusterId") final long clusterId) {
        final List<DatasetChangeLogRegistered> changeLogs = this.dataSetService.getDatasetChangeLogs(clusterId);
        return new ResponseEntity<List<DatasetChangeLogRegistered>>(changeLogs, HttpStatus.OK);
    }

    @ApiOperation(value = "View a list of available datasets based on the prefix ", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved list"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("dataSets/")
    public ResponseEntity<List<CumulativeDataset>> getAllDataSets(
            @RequestParam(value = "prefix") final String dataSetSubString) {
        final List<CumulativeDataset> list = this.dataSetService.getAllDatasets(dataSetSubString);
        return new ResponseEntity<List<CumulativeDataset>>(list, HttpStatus.OK);
    }

    @ApiOperation(value = "View a list of available detailed datasets based on the prefix ", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved list"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("detailedDataSets/")
    public ResponseEntity<List<CumulativeDataset>> getAllDetailedDataSets(
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
    public ResponseEntity<Page<Dataset>> getAllPendingDataSetsBySystemName(
            @PathVariable("storageSystemName") final String storageSystemName,
            @PathVariable("storageTypeName") final String storageTypeName,
            @RequestParam(defaultValue = "All") final String datasetStr,
            @RequestParam(defaultValue = "0") final int page,
            @RequestParam(defaultValue = "3") final int size) {
        final Pageable pageable = new PageRequest(page, size);
        final Page<Dataset> list = this.dataSetService.getPendingDatasets(datasetStr, storageTypeName,
                storageSystemName, pageable);
        return new ResponseEntity<Page<Dataset>>(list, HttpStatus.OK);
    }

    @ApiOperation(value = "View a list of available datasets based on storage system Name and type name", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved list"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("dataSetsBySystem/{storageTypeName:.+}/{storageSystemName:.+}")
    public ResponseEntity<Page<Dataset>> getAllDataSetsBySystemName(
            @PathVariable("storageTypeName") final String storageTypeName,
            @PathVariable("storageSystemName") final String storageSystemName,
            @RequestParam(defaultValue = "All") final String datasetStr,
            @RequestParam(defaultValue = "0") final int page,
            @RequestParam(defaultValue = "3") final int size) {

        final Pageable pageable = new PageRequest(page, size);
        final Page<Dataset> list = this.dataSetService.getAllDatasetsByTypeAndSystem(datasetStr, storageTypeName,
                storageSystemName, pageable);
        return new ResponseEntity<Page<Dataset>>(list, HttpStatus.OK);
    }

    @ApiOperation(value = "View a list of deactivated datasets based on storage system Name and type name", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved list"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("dataSetsBySystemDeleted/{storageTypeName:.+}/{storageSystemName:.+}")
    public ResponseEntity<Page<Dataset>> getAllDeactivatedDataSetsBySystemName(
            @PathVariable("storageTypeName") final String storageTypeName,
            @PathVariable("storageSystemName") final String storageSystemName,
            @RequestParam(defaultValue = "All") final String datasetStr,
            @RequestParam(defaultValue = "0") final int page,
            @RequestParam(defaultValue = "3") final int size) {

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
            final Dataset insertedDataset = this.dataSetService.addDataset(dataSet);
            return new ResponseEntity<String>(this.gson.toJson(insertedDataset.getStorageDataSetId()), HttpStatus.OK);
        }
        catch (final ValidationError e) {
            return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
        }
    }

    @ApiOperation(value = "Get Sample Data from dataset", response = String.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved data"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("/sampleData/{datasetName:.+}/{objectId}")
    public ResponseEntity<?> getSampleData(@PathVariable("datasetName") final String datasetName,
            @PathVariable("objectId") final long objectId) {

        if (this.isProd.equals("false")) {
            final List<List<String>> tempOutput = new ArrayList<List<String>>();
            tempOutput.add(new ArrayList<String>(Arrays.asList("c1", "c2", "c3", "c4", "c5")));
            tempOutput.add(new ArrayList<String>(Arrays.asList("a", "b", "c", "d", "e")));
            tempOutput.add(new ArrayList<String>(Arrays.asList("m", "n", "o", "p", "q")));
            return new ResponseEntity<List<List<String>>>(tempOutput, HttpStatus.OK);
        }

        try {
            final List<List<String>> outputs = this.dataSetService.getSampleData(datasetName, objectId);
            return new ResponseEntity<List<List<String>>>(outputs, HttpStatus.OK);

        }
        catch (final ValidationError e) {
            return new ResponseEntity<ValidationError>(e, HttpStatus.OK);
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
            final Dataset insertedDataset = this.dataSetService.addDataset(dataSet);
            return new ResponseEntity<String>(this.gson.toJson(insertedDataset.getStorageDataSetId()), HttpStatus.OK);
        }
        catch (final ValidationError e) {
            return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
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
            final Dataset dataset = this.dataSetService.deleteDataSet(id);
            return new ResponseEntity<String>(this.gson.toJson("Deleted " + dataset.getStorageDataSetId()),
                    HttpStatus.OK);
        }
        catch (final ValidationError e) {
            return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
        }
    }

    // @ApiOperation(value = "Update Dataset Deployment Status upon success", response = String.class)
    // @ApiResponses(value = {
    // @ApiResponse(code = 200, message = "Successfully updated dataset"),
    // @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    // })
    // @PostMapping("datasetDeployment/{datasetChangeLogId}")
    // public ResponseEntity<String> updateDatasetAfterDeployment(
    // @PathVariable("datasetChangeLogId") final long datasetChangeLogId) {
    // try {
    // final long datasetId = this.dataSetService.updateDatasetChangeLogs(datasetChangeLogId);
    // return new ResponseEntity<String>(this.gson.toJson("Updated Dataset " + datasetId), HttpStatus.OK);
    // }
    // catch (final ValidationError e) {
    // return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
    // }
    // }

    // @ApiOperation(value = "Update Dataset Deployment Status upon failure", response = String.class)
    // @ApiResponses(value = {
    // @ApiResponse(code = 200, message = "Successfully updated dataset"),
    // @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    // })
    // @PostMapping("datasetDeploymentFailure/{datasetChangeLogId}")
    // public ResponseEntity<String> updateDatasetAfterDeploymentForFailure(
    // @PathVariable("datasetChangeLogId") final long datasetChangeLogId) {
    // try {
    // final long datasetId = this.dataSetService.updateDatasetChangeLogOnFailure(datasetChangeLogId);
    // return new ResponseEntity<String>(this.gson.toJson("Updated Dataset " + datasetId), HttpStatus.OK);
    // }
    // catch (final ValidationError e) {
    // return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
    // }
    // }
}
