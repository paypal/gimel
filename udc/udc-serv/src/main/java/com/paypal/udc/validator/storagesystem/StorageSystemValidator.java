package com.paypal.udc.validator.storagesystem;

import com.paypal.udc.entity.storagesystem.StorageSystem;
import com.paypal.udc.exception.ValidationError;


public interface StorageSystemValidator {

    void setNextChain(StorageSystemValidator nextChain);

    void validate(StorageSystem storageSystem, final StorageSystem updatedStorageSystem) throws ValidationError;
}
