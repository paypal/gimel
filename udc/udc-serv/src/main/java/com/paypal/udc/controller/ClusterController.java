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
import com.paypal.udc.entity.Cluster;
import com.paypal.udc.exception.ValidationError;
import com.paypal.udc.service.IClusterService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;


@RestController
@RequestMapping("cluster")
@Api(value = "Cluster Services", description = "Operations pertaining to Cluster Service")
public class ClusterController {

    final static Logger logger = LoggerFactory.getLogger(ClusterController.class);

    final Gson gson = new Gson();
    @Autowired
    private IClusterService clusterService;

    @ApiOperation(value = "View the Cluster based on ID", response = Cluster.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved Cluster"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("cluster/{id}")
    public ResponseEntity<Cluster> getClusterById(@PathVariable("id") final Long id) {
        final Cluster cluster = this.clusterService.getClusterById(id);
        return new ResponseEntity<Cluster>(cluster, HttpStatus.OK);
    }

    @ApiOperation(value = "View the Cluster based on Name", response = Cluster.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved Cluster"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("clusterByName/{name:.+}")
    public ResponseEntity<Cluster> getClusterByName(@PathVariable("name") final String name) {
        final Cluster storage = this.clusterService.getClusterByName(name);
        return new ResponseEntity<Cluster>(storage, HttpStatus.OK);
    }

    @ApiOperation(value = "View a list of available Cluster", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved list"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("clusters")
    public ResponseEntity<List<Cluster>> getAllClusters() {

        final List<Cluster> list = this.clusterService.getAllClusters();
        return new ResponseEntity<List<Cluster>>(list, HttpStatus.OK);
    }

    @ApiOperation(value = "Insert a Cluster", response = Cluster.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully inserted Cluster"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PostMapping("cluster")
    public ResponseEntity<?> addStorage(@RequestBody final Cluster cluster) {
        Cluster insertedCluster;
        try {
            insertedCluster = this.clusterService.addCluster(cluster);
            return new ResponseEntity<Cluster>(insertedCluster, HttpStatus.OK);
        }
        catch (final ValidationError e) {
            return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
        }
    }

    @ApiOperation(value = "Update an Cluster based on Input", response = String.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully updated Cluster"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PutMapping("cluster")
    public ResponseEntity<String> updateStorage(@RequestBody final Cluster cluster) {
        Cluster updatedCluster;
        try {
            updatedCluster = this.clusterService.updateCluster(cluster);
            return new ResponseEntity<String>(this.gson.toJson(updatedCluster), HttpStatus.OK);
        }
        catch (final ValidationError e) {
            return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
        }
    }

    @ApiOperation(value = "Deactivate a Cluster on ID")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully deActivated Cluster"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @DeleteMapping("dcluster/{id}")
    public ResponseEntity<String> deActivateCluster(@PathVariable("id") final Integer id) {
        try {
            final Cluster cluster = this.clusterService.deActivateCluster(id);
            return new ResponseEntity<String>(this.gson.toJson("Deactivated " + cluster.getClusterId()), HttpStatus.OK);
        }
        catch (final ValidationError e) {
            return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
        }

    }

    @ApiOperation(value = "Reactivate a Cluster on ID")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully reactivated Cluster"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PutMapping("ecluster/{id}")
    public ResponseEntity<String> reActivateCluster(@PathVariable("id") final Integer id) {
        try {
            final Cluster cluster = this.clusterService.reActivateCluster(id);
            return new ResponseEntity<String>(this.gson.toJson("Reactivated " + cluster.getClusterId()), HttpStatus.OK);
        }
        catch (final ValidationError e) {
            return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
        }

    }
}
