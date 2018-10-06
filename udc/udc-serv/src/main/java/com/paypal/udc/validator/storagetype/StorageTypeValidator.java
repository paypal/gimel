package com.paypal.udc.validator.storagetype;

import com.paypal.udc.entity.storagetype.StorageType;
import com.paypal.udc.exception.ValidationError;


public interface StorageTypeValidator {

    void setNextChain(StorageTypeValidator nextChain);

    void validate(StorageType storageType, final StorageType updatedStorageType) throws ValidationError;
}
