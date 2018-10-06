package com.paypal.udc.cache;

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Component;
import com.paypal.udc.dao.storagesystem.StorageSystemRepository;
import com.paypal.udc.entity.storagesystem.StorageSystem;
import com.paypal.udc.entity.storagesystem.StorageSystemAttributeValue;
import com.paypal.udc.service.impl.StorageSystemService;


@Component
public class StorageSystemCache {

    final static Logger logger = LoggerFactory.getLogger(StorageSystemCache.class);

    @Autowired
    StorageSystemRepository storageSystemRepository;

    @Autowired
    StorageSystemService storageSystemService;

    @Cacheable(value = "storageSystemCache", key = "#storageSystemId")
    public StorageSystem getStorageSystem(final Long storageSystemId) {
        // return storageSystemRepository.findOne(storageSystemId);
        return this.storageSystemService.getStorageSystemById(storageSystemId);
    }

    @Cacheable(value = "storageSystemAttributeCache", key = "#storageSystemId")
    public List<StorageSystemAttributeValue> getAttributeValues(final Long storageSystemId) {
        return this.storageSystemService.getStorageSystemAttributes(storageSystemId);
    }

}
