package com.paypal.udc.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Component;
import com.paypal.udc.entity.storagetype.StorageType;
import com.paypal.udc.service.impl.StorageTypeService;


@Component
public class StorageTypeCache {

    final static Logger logger = LoggerFactory.getLogger(StorageTypeCache.class);

    @Autowired
    StorageTypeService storageTypeService;

    @Cacheable(value = "storageTypeCache", key = "#storageTypeId")
    public StorageType getStorageType(final Long storageTypeId) {
        return this.storageTypeService.getStorageTypeById(storageTypeId);
    }

}
