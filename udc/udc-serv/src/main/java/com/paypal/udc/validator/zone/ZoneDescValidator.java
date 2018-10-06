package com.paypal.udc.validator.zone;

import org.springframework.stereotype.Component;
import com.paypal.udc.entity.Zone;
import com.paypal.udc.exception.ValidationError;


@Component
public class ZoneDescValidator implements ZoneValidator {

    private ZoneValidator chain;

    @Override
    public void setNextChain(final ZoneValidator nextChain) {
        this.chain = nextChain;
    }

    @Override
    public void validate(final Zone zone, final Zone updatedZone) throws ValidationError {
        if (zone.getZoneDescription() != null && zone.getZoneDescription().length() >= 0) {
            updatedZone.setZoneDescription(zone.getZoneDescription());
        }
    }
}
