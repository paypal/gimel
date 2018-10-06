package com.paypal.udc.service;

import java.util.List;
import com.paypal.udc.entity.Zone;
import com.paypal.udc.exception.ValidationError;


public interface IZoneService {

    List<Zone> getAllZones();

    Zone getZoneById(long zoneId);

    Zone addZone(Zone zone) throws ValidationError;

    Zone updateZone(Zone zone) throws ValidationError;

    Zone deActivateZone(long zoneId) throws ValidationError;

    Zone getZoneByName(String zoneName);

    Zone reActivateZone(long zoneId) throws ValidationError;

}
