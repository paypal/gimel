package com.paypal.udc.validator.storagetype;

import org.springframework.stereotype.Component;
import com.paypal.udc.entity.storagetype.StorageType;
import com.paypal.udc.exception.ValidationError;


@Component
public class StorageTypeDescValidator implements StorageTypeValidator {

    private StorageTypeValidator chain;

    @Override
    public void setNextChain(final StorageTypeValidator nextChain) {
        this.chain = nextChain;
    }

    @Override
    public void validate(final StorageType storageType, final StorageType updatedStorageType)
            throws ValidationError {
        if (storageType.getStorageTypeDescription() != null && storageType.getStorageTypeDescription().length() >= 0) {
            updatedStorageType.setStorageTypeDescription(storageType.getStorageTypeDescription());
        }
        this.chain.validate(storageType, updatedStorageType);
    }

}
