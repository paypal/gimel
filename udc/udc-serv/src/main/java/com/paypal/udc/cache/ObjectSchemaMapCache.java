package com.paypal.udc.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Component;
import com.paypal.udc.dao.objectschema.ObjectSchemaMapRepository;
import com.paypal.udc.entity.objectschema.ObjectSchemaMap;


@Component
public class ObjectSchemaMapCache {
    final static Logger logger = LoggerFactory.getLogger(ObjectSchemaMapCache.class);

    @Autowired
    ObjectSchemaMapRepository objectSchemaMapRepository;

    @Cacheable(value = "objectSchemaMapCache", key = "#objectId")
    public ObjectSchemaMap getObject(final long objectId) {
        return this.objectSchemaMapRepository.findOne(objectId);
    }
}
