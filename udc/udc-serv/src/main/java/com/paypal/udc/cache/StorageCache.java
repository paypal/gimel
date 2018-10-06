package com.paypal.udc.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Component;
import com.paypal.udc.entity.Storage;
import com.paypal.udc.service.impl.StorageService;


@Component
public class StorageCache {

    final static Logger logger = LoggerFactory.getLogger(StorageCache.class);

    @Autowired
    StorageService storageService;

    @Cacheable(value = "storageCache", key = "#storageId")
    public Storage getStorage(final Long storageId) {
        return this.storageService.getStorageById(storageId);
    }

}
