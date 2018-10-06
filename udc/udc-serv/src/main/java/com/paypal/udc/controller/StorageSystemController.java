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
import com.paypal.udc.cache.StorageSystemCache;
import com.paypal.udc.entity.storagesystem.StorageSystem;
import com.paypal.udc.entity.storagesystem.StorageSystemAttributeValue;
import com.paypal.udc.exception.ValidationError;
import com.paypal.udc.service.IStorageSystemService;
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
    @Autowired
    private IStorageSystemService storageSystemService;
    @Autowired
    private StorageSystemCache storageSystemCache;

    @ApiOperation(value = "View the Storage System based on System ID", response = StorageSystem.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved Storage System"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("storageSystem/{id}")
    public ResponseEntity<StorageSystem> getStorageSystemById(@PathVariable("id") final Long id) {
        final StorageSystem storageSystem = this.storageSystemCache.getStorageSystem(id);
        return new ResponseEntity<StorageSystem>(storageSystem, HttpStatus.OK);
    }

    @ApiOperation(value = "View the Storage System Attributes based on System ID", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved Storage System Attributes"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("storageSystemAttributes/{id}")
    public ResponseEntity<List<StorageSystemAttributeValue>> getStorageAttributesById(
            @PathVariable("id") final Long id) {
        final List<StorageSystemAttributeValue> storageAttributes = this.storageSystemCache.getAttributeValues(id);
        return new ResponseEntity<List<StorageSystemAttributeValue>>(storageAttributes, HttpStatus.OK);
    }

    @ApiOperation(value = "View the Storage System Attributes based on Storage System Name", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved Storage System Attributes"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("storageSystemAttributesByName/{storageSystemName:.+}")
    public ResponseEntity<List<StorageSystemAttributeValue>> getStorageAttributesByName(
            @PathVariable("storageSystemName") final String storageSystemName) {
        final List<StorageSystemAttributeValue> storageAttributes = this.storageSystemService
                .getAttributeValuesByName(storageSystemName);
        return new ResponseEntity<List<StorageSystemAttributeValue>>(storageAttributes, HttpStatus.OK);
    }

    @ApiOperation(value = "View the Storage System based on Storage Type ID", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved Storage"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("storageSystemByType/{storageTypeId}")
    public ResponseEntity<List<StorageSystem>> getStorageSystemByType(
            @PathVariable("storageTypeId") final long storageTypeId) {
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
    public ResponseEntity<?> getStorageSystemByTypeName(
            @PathVariable("storageTypeName") final String storageTypeName) {
        List<StorageSystem> storageSystems;
        try {
            storageSystems = this.storageSystemService.getStorageSystemByType(storageTypeName);
            return new ResponseEntity<List<StorageSystem>>(storageSystems, HttpStatus.OK);
        }
        catch (final ValidationError e) {
            return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
        }

    }

    @ApiOperation(value = "View a list of available Storage Systems", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved list"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("storageSystems")
    public ResponseEntity<List<StorageSystem>> getAllStorageSystems() {
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
            insertedStorageSystem = this.storageSystemService.addStorageSystem(storageSystem);
            return new ResponseEntity<StorageSystem>(insertedStorageSystem, HttpStatus.CREATED);
        }
        catch (final ValidationError e) {
            return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
        }
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
            updatedStorageSystem = this.storageSystemService.updateStorageSystem(storageSystem);
            return new ResponseEntity<StorageSystem>(updatedStorageSystem, HttpStatus.OK);
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
    public ResponseEntity<String> deleteStorageSystem(@PathVariable("id") final long id) {
        try {
            final StorageSystem storageSystem = this.storageSystemService.deleteStorageSystem(id);
            return new ResponseEntity<String>(this.gson.toJson("Deactivated " + storageSystem.getStorageSystemId()),
                    HttpStatus.OK);
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
    public ResponseEntity<String> enableStorageSystem(@PathVariable("id") final long id) {
        try {
            final StorageSystem storageSystem = this.storageSystemService.enableStorageSystem(id);
            return new ResponseEntity<String>(this.gson.toJson("Reactivated " + storageSystem.getStorageSystemId()),
                    HttpStatus.OK);
        }
        catch (final ValidationError e) {
            return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
        }
    }
}
