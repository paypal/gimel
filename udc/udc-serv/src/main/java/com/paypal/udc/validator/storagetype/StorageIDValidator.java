package com.paypal.udc.validator.storagetype;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import com.paypal.udc.entity.storagetype.StorageType;
import com.paypal.udc.exception.ValidationError;
import com.paypal.udc.util.StorageUtil;


@Component
public class StorageIDValidator implements StorageTypeValidator {

    private StorageTypeValidator chain;

    @Autowired
    private StorageUtil storageUtil;

    @Override
    public void setNextChain(final StorageTypeValidator nextChain) {
        this.chain = nextChain;
    }

    @Override
    public void validate(final StorageType storageType, final StorageType updatedStorageType)
            throws ValidationError {
        if (storageType.getStorageId() > 0) {
            this.storageUtil.validateStorageId(storageType.getStorageId());
            updatedStorageType.setStorageId(storageType.getStorageId());
        }
    }
}
