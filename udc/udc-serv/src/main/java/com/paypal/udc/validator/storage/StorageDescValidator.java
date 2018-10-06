package com.paypal.udc.validator.storage;

import org.springframework.stereotype.Component;
import com.paypal.udc.entity.Storage;
import com.paypal.udc.exception.ValidationError;


@Component
public class StorageDescValidator implements StorageValidator {

    private StorageValidator chain;

    @Override
    public void setNextChain(final StorageValidator nextChain) {
        this.chain = nextChain;
    }

    @Override
    public void validate(final Storage storage, final Storage updatedStorage) throws ValidationError {
        if (storage.getStorageDescription() != null && storage.getStorageDescription().length() >= 0) {
            updatedStorage.setStorageDescription(storage.getStorageDescription());
        }
    }
}
