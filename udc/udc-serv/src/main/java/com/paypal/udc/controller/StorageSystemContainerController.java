package com.paypal.udc.controller;

import java.util.List;
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
import com.paypal.udc.entity.storagesystem.CollectiveStorageSystemContainerObject;
import com.paypal.udc.entity.storagesystem.StorageSystemContainer;
import com.paypal.udc.exception.ValidationError;
import com.paypal.udc.service.IStorageSystemContainerService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;


@RestController
@RequestMapping("storageSystemContainer")
@Api(value = "StorageSystemContainerService", description = "Operations pertaining to Storage System Container")
public class StorageSystemContainerController {

    final static Logger logger = LoggerFactory.getLogger(StorageSystemContainerController.class);

    final Gson gson = new Gson();
    @Autowired
    private IStorageSystemContainerService storageSystemContainerService;

    @ApiOperation(value = "View the Storage System Container based on ID", response = StorageSystemContainer.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved Storage System Container"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("storageSystemContainer/{id}")
    public ResponseEntity<StorageSystemContainer> getClusterById(@PathVariable("id") final Long id) {
        final StorageSystemContainer cluster = this.storageSystemContainerService.getStorageSystemContainerById(id);
        return new ResponseEntity<StorageSystemContainer>(cluster, HttpStatus.OK);
    }

    @ApiOperation(value = "View a list of available Storage System Containers", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved list"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("storageSystemContainers/{clusterId}")
    public ResponseEntity<List<CollectiveStorageSystemContainerObject>> getAllStorageSystemContainers(
            @PathVariable("clusterId") final long clusterId) {
        final List<CollectiveStorageSystemContainerObject> list = this.storageSystemContainerService
                .getAllStorageSystemContainers(clusterId);
        return new ResponseEntity<List<CollectiveStorageSystemContainerObject>>(list, HttpStatus.OK);
    }

    @ApiOperation(value = "For a given Storage System ID - view a list of available Storage System Containers", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved list"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("storageSystemContainersByStorageSystem/{id}")
    public ResponseEntity<List<StorageSystemContainer>> getAllStorageSystemContainersByStorageSystemId(
            @PathVariable("id") final long storageSystemId) {
        final List<StorageSystemContainer> list = this.storageSystemContainerService
                .getStorageSystemContainersByStorageSystemId(storageSystemId);
        return new ResponseEntity<List<StorageSystemContainer>>(list, HttpStatus.OK);
    }

    @ApiOperation(value = "Delete an Storage System Container By ID")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully deleted Cluster"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @DeleteMapping("storageSystemContainer/{id}")
    public ResponseEntity<String> deleteStorage(@PathVariable("id") final long id) {
        try {
            final StorageSystemContainer storageSystemContainer = this.storageSystemContainerService
                    .deleteStorageSystemContainer(id);
            return new ResponseEntity<String>(
                    this.gson.toJson("Deactivated " + storageSystemContainer.getStorageSystemContainerId()),
                    HttpStatus.OK);
        }
        catch (final ValidationError e) {
            return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
        }
    }

    @ApiOperation(value = "Insert a Storage System Container", response = StorageSystemContainer.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully inserted Storage System Container"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PostMapping("storageSystemContainer")
    public ResponseEntity<?> addStorageSystemContainer(
            @RequestBody final StorageSystemContainer storageSystemContainer) {
        StorageSystemContainer insertedStorageSystemContainer;
        try {
            insertedStorageSystemContainer = this.storageSystemContainerService
                    .addStorageSystemContainer(storageSystemContainer);
            return new ResponseEntity<StorageSystemContainer>(insertedStorageSystemContainer, HttpStatus.OK);
        }
        catch (final ValidationError e) {
            return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
        }
    }

    @ApiOperation(value = "Update an Storage System Container based on Input", response = String.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully updated Storage System Container"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PutMapping("storageSystemContainer")
    public ResponseEntity<String> updateStorageSystemContainer(
            @RequestBody final StorageSystemContainer storageSystemContainer) {
        StorageSystemContainer updatedStorageSystemContainer;
        try {
            updatedStorageSystemContainer = this.storageSystemContainerService
                    .updateStorageSystemContainer(storageSystemContainer);
            return new ResponseEntity<String>(this.gson.toJson(updatedStorageSystemContainer), HttpStatus.OK);
        }
        catch (final ValidationError e) {
            return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
        }
    }

}
