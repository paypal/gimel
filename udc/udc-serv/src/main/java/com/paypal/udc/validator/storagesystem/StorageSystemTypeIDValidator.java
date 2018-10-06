package com.paypal.udc.validator.storagesystem;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import com.paypal.udc.entity.storagesystem.StorageSystem;
import com.paypal.udc.exception.ValidationError;
import com.paypal.udc.util.StorageTypeUtil;


@Component
public class StorageSystemTypeIDValidator implements StorageSystemValidator {

    private StorageSystemValidator chain;

    @Autowired
    private StorageTypeUtil storageTypeUtil;

    @Override
    public void setNextChain(final StorageSystemValidator nextChain) {
        this.chain = nextChain;
    }

    @Override
    public void validate(final StorageSystem storageSystem, final StorageSystem updatedStorageSystem)
            throws ValidationError {
        if (storageSystem.getStorageTypeId() > 0) {
            this.storageTypeUtil.validateStorageTypeId(storageSystem.getStorageTypeId());
            updatedStorageSystem.setStorageTypeId(storageSystem.getStorageTypeId());
        }
    }

}
