package com.paypal.udc.validator.storagesystem;

import org.springframework.stereotype.Component;
import com.paypal.udc.entity.storagesystem.StorageSystem;
import com.paypal.udc.exception.ValidationError;


@Component
public class StorageSystemDescValidator implements StorageSystemValidator {

    private StorageSystemValidator chain;

    @Override
    public void setNextChain(final StorageSystemValidator nextChain) {
        this.chain = nextChain;
    }

    @Override
    public void validate(final StorageSystem storageSystem, final StorageSystem updatedStorageSystem)
            throws ValidationError {
        if (storageSystem.getStorageSystemDescription() != null
                && storageSystem.getStorageSystemDescription().length() >= 0) {
            updatedStorageSystem.setStorageSystemDescription(storageSystem.getStorageSystemDescription());
        }
        this.chain.validate(storageSystem, updatedStorageSystem);
    }

}
