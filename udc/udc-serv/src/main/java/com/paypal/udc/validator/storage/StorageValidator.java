package com.paypal.udc.validator.storage;

import com.paypal.udc.entity.Storage;
import com.paypal.udc.exception.ValidationError;


public interface StorageValidator {

    void setNextChain(StorageValidator nextChain);

    void validate(Storage storage, final Storage updatedStorage) throws ValidationError;
}
