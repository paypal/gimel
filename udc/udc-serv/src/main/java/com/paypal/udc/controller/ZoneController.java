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
import com.paypal.udc.entity.Zone;
import com.paypal.udc.exception.ValidationError;
import com.paypal.udc.service.IZoneService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;


@RestController
@RequestMapping("zone")
@Api(value = "Zone Services", description = "Operations pertaining to Zone Service")
public class ZoneController {

    final static Logger logger = LoggerFactory.getLogger(ZoneController.class);

    final Gson gson = new Gson();
    @Autowired
    private IZoneService zoneService;

    @ApiOperation(value = "View the zone based on ID", response = Zone.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved zone"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("zone/{id}")
    public ResponseEntity<Zone> getZoneById(@PathVariable("id") final Long id) {
        final Zone zone = this.zoneService.getZoneById(id);
        return new ResponseEntity<Zone>(zone, HttpStatus.OK);
    }

    @ApiOperation(value = "View the Zone based on Name", response = Zone.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved Zone"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("zoneByName/{name:.+}")
    public ResponseEntity<Zone> getZoneByName(@PathVariable("name") final String name) {
        final Zone zone = this.zoneService.getZoneByName(name);
        return new ResponseEntity<Zone>(zone, HttpStatus.OK);
    }

    @ApiOperation(value = "View a list of available zone", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved list"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("zones")
    public ResponseEntity<List<Zone>> getAllZones() {

        final List<Zone> list = this.zoneService.getAllZones();
        return new ResponseEntity<List<Zone>>(list, HttpStatus.OK);
    }

    @ApiOperation(value = "Insert a Zone", response = Zone.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully inserted Zone"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PostMapping("zone")
    public ResponseEntity<?> addZone(@RequestBody final Zone zone) {
        final Zone insertedZone;
        try {
            insertedZone = this.zoneService.addZone(zone);
            return new ResponseEntity<Zone>(insertedZone, HttpStatus.OK);
        }
        catch (final ValidationError e) {
            return new ResponseEntity<ValidationError>(e, e.getErrorCode());
        }
    }

    @ApiOperation(value = "Update an Zone based on Input", response = Zone.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully updated Zone"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PutMapping("zone")
    public ResponseEntity<?> updateZone(@RequestBody final Zone zone) {
        Zone updatedZone;
        try {
            updatedZone = this.zoneService.updateZone(zone);
            return new ResponseEntity<Zone>(updatedZone, HttpStatus.OK);
        }
        catch (final ValidationError e) {
            return new ResponseEntity<ValidationError>(e, e.getErrorCode());
        }
    }

    @ApiOperation(value = "Deactivate a Zone on ID")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully deActivated Zone"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @DeleteMapping("dzone/{id}")
    public ResponseEntity<String> deactivateZone(@PathVariable("id") final Integer id) {
        try {
            final Zone zone = this.zoneService.deActivateZone(id);
            return new ResponseEntity<String>(this.gson.toJson("Deactivated " + zone.getZoneId()), HttpStatus.OK);
        }
        catch (final ValidationError e) {
            return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
        }

    }

    @ApiOperation(value = "Reactivate a Zone on ID")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully reactivated zone"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PutMapping("ezone/{id}")
    public ResponseEntity<String> reactivateZone(@PathVariable("id") final Integer id) {
        try {
            final Zone zone = this.zoneService.reActivateZone(id);
            return new ResponseEntity<String>(this.gson.toJson("Reactivated " + zone.getZoneId()), HttpStatus.OK);
        }
        catch (final ValidationError e) {
            return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
        }

    }
}
