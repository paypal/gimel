package com.paypal.udc.util;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import com.paypal.udc.cache.StorageCache;
import com.paypal.udc.entity.Storage;
import com.paypal.udc.exception.ValidationError;


@Component
public class StorageUtil {

    @Autowired
    private StorageCache storageCache;

    public void validateStorageId(final long id) throws ValidationError {
        final Storage storage = this.storageCache.getStorage(id);
        if (storage == null) {
            final ValidationError verror = new ValidationError();
            verror.setErrorCode(HttpStatus.BAD_REQUEST);
            verror.setErrorDescription("Invalid Storage ID");
            throw verror;
        }
    }

}
