package com.paypal.udc.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Component;
import com.paypal.udc.dao.dataset.DatasetRepository;
import com.paypal.udc.entity.dataset.Dataset;
import com.paypal.udc.service.impl.DatasetService;


@Component
public class DatasetCache {

    final static Logger logger = LoggerFactory.getLogger(DatasetCache.class);

    @Autowired
    DatasetRepository dataSetRepository;

    @Autowired
    DatasetService dataSetService;

    @Cacheable(value = "dataSetCache", key = "#storageDataSetId")
    public Dataset getDataSet(final Long storageDataSetId) {
        return this.dataSetService.getDataSetById(storageDataSetId);
    }
}
