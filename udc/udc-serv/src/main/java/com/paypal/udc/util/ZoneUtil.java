package com.paypal.udc.util;

import java.util.HashMap;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import com.paypal.udc.dao.ZoneRepository;
import com.paypal.udc.entity.Zone;
import com.paypal.udc.exception.ValidationError;


@Component
public class ZoneUtil {

    @Autowired
    private ZoneRepository zoneRepository;

    public void validateZone(final long zoneId) throws ValidationError {
        final Zone zone = this.zoneRepository.findOne(zoneId);
        if (zone == null) {
            final ValidationError v = new ValidationError();
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("ZoneID is incorrect");
            throw v;
        }
    }

    public Map<Long, Zone> getZones() {
        final Map<Long, Zone> zones = new HashMap<Long, Zone>();
        this.zoneRepository.findAll()
                .forEach(zone -> {
                    zones.put(zone.getZoneId(), zone);
                });
        return zones;
    }
}
