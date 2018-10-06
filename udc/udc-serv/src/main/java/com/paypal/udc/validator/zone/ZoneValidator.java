package com.paypal.udc.validator.zone;

import com.paypal.udc.entity.Zone;
import com.paypal.udc.exception.ValidationError;


public interface ZoneValidator {

    void setNextChain(ZoneValidator nextChain);

    void validate(final Zone zone, final Zone updatedZone) throws ValidationError;
}
