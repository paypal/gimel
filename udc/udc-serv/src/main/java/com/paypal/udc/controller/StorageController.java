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
import org.springframework.web.util.UriComponentsBuilder;
import com.google.gson.Gson;
import com.paypal.udc.cache.StorageCache;
import com.paypal.udc.entity.Storage;
import com.paypal.udc.exception.ValidationError;
import com.paypal.udc.service.IStorageService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;


@RestController
@RequestMapping("storage")
@Api(value = "StorageCategoryService", description = "Operations pertaining to Storage Category")
public class StorageController {

    final static Logger logger = LoggerFactory.getLogger(StorageController.class);

    final Gson gson = new Gson();
    @Autowired
    private IStorageService storageService;
    @Autowired
    private StorageCache storageCache;

    @ApiOperation(value = "View the Storage Category based on ID", response = Storage.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved Storage"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("storage/{id}")
    public ResponseEntity<Storage> getStorageById(@PathVariable("id") final Long id) {
        final Storage storage = this.storageCache.getStorage(id);
        return new ResponseEntity<Storage>(storage, HttpStatus.OK);
    }

    @ApiOperation(value = "View the Storage Category based on Name", response = Storage.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved Storage"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("storageByName/{name:.+}")
    public ResponseEntity<Storage> getStorageByTitle(@PathVariable("name") final String name) {
        final Storage storage = this.storageService.getStorageByName(name);
        return new ResponseEntity<Storage>(storage, HttpStatus.OK);
    }

    @ApiOperation(value = "View a list of available Storage Categories", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved list"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("storages")
    public ResponseEntity<List<Storage>> getAllStorages() {
        final List<Storage> list = this.storageService.getAllStorages();
        return new ResponseEntity<List<Storage>>(list, HttpStatus.OK);
    }

    @ApiOperation(value = "Insert an Storage Category", response = String.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully inserted Storage Category"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PostMapping("storage")
    public ResponseEntity<String> addStorage(@RequestBody final Storage storage, final UriComponentsBuilder builder) {
        Storage insertedStorage;
        try {
            insertedStorage = this.storageService.addStorage(storage);
            return new ResponseEntity<String>(this.gson.toJson(insertedStorage), HttpStatus.OK);
        }
        catch (final ValidationError e) {
            return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
        }
    }

    @ApiOperation(value = "Update an Storage Category based on Input", response = String.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully updated Storage"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PutMapping("storage")
    public ResponseEntity<String> updateStorage(@RequestBody final Storage storage) {
        Storage updatedStorage;
        try {
            updatedStorage = this.storageService.updateStorage(storage);
            return new ResponseEntity<String>(this.gson.toJson(updatedStorage), HttpStatus.OK);
        }
        catch (final ValidationError e) {
            return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
        }
    }

    @ApiOperation(value = "Delete an Storage based on ID")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully deleted Storage"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @DeleteMapping("dstorage/{id}")
    public ResponseEntity<String> deleteStorage(@PathVariable("id") final Long id) {
        try {
            final Storage storage = this.storageService.deleteStorage(id);
            return new ResponseEntity<String>(this.gson.toJson("Deactivated " + storage.getStorageId()), HttpStatus.OK);
        }
        catch (final ValidationError e) {
            return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
        }
    }

    @ApiOperation(value = "Enable an Storage based on ID")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully Enabled Storage"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PutMapping("estorage/{id}")
    public ResponseEntity<String> enableStorage(@PathVariable("id") final Long id) {
        try {
            final Storage storage = this.storageService.enableStorage(id);
            return new ResponseEntity<String>(this.gson.toJson("Enabled " + storage.getStorageId()), HttpStatus.OK);
        }
        catch (final ValidationError e) {
            return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
        }

    }

}
