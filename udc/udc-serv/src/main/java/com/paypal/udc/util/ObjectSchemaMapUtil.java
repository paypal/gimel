package com.paypal.udc.util;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.paypal.udc.dao.objectschema.ObjectSchemaAttributeValueRepository;
import com.paypal.udc.dao.objectschema.ObjectSchemaMapRepository;
import com.paypal.udc.dao.storagetype.StorageTypeAttributeKeyRepository;
import com.paypal.udc.entity.objectschema.CollectiveObjectAttributeValue;
import com.paypal.udc.entity.objectschema.CollectiveObjectSchemaMap;
import com.paypal.udc.entity.objectschema.ObjectAttributeValue;
import com.paypal.udc.entity.objectschema.ObjectSchemaMap;
import com.paypal.udc.entity.objectschema.Schema;
import com.paypal.udc.entity.storagetype.StorageType;
import com.paypal.udc.entity.storagetype.StorageTypeAttributeKey;
import com.paypal.udc.exception.ValidationError;
import com.paypal.udc.util.enumeration.ActiveEnumeration;


@Component
public class ObjectSchemaMapUtil {

    @Autowired
    private StorageTypeAttributeKeyRepository stakr;
    @Autowired
    private ObjectSchemaMapRepository schemaMapRepository;
    @Autowired
    private StorageSystemUtil systemUtil;

    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss");

    public StorageTypeAttributeKey getStorageTypeAttributeValue(final String attributeName) {
        final StorageTypeAttributeKey attributeKey = this.stakr.findByStorageDsAttributeKeyName(attributeName);
        return attributeKey;
    }

    public void updateObject(final long objectSchemaMapId) throws ValidationError {

        final ObjectSchemaMap schemaMap = this.schemaMapRepository.findOne(objectSchemaMapId);
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final String time = sdf.format(timestamp);
        if (schemaMap == null) {
            final ValidationError verror = new ValidationError();
            verror.setErrorCode(HttpStatus.CONFLICT);
            verror.setErrorDescription("Invalid Schema Map Id");
            throw verror;
        }
        schemaMap.setUpdatedTimestamp(time);
        this.schemaMapRepository.save(schemaMap);
    }

    public ObjectSchemaMap validateObjectSchemaMapId(final long objectSchemaMapId) throws ValidationError {
        final ObjectSchemaMap osm = this.schemaMapRepository.findOne(objectSchemaMapId);
        if (osm == null) {
            final ValidationError verror = new ValidationError();
            verror.setErrorCode(HttpStatus.CONFLICT);
            verror.setErrorDescription("Invalid Schema Map Id");
            throw verror;
        }
        return osm;
    }

    public ObjectSchemaMap validateSystemContainerObjectMapping(final long storageSystemId,
            final String storageContainerName, final String objectName) throws ValidationError {
        final ObjectSchemaMap objectSchemaMap = this.schemaMapRepository
                .findByStorageSystemIdAndContainerNameAndObjectName(storageSystemId, storageContainerName, objectName);
        if (objectSchemaMap == null) {
            final ValidationError verror = new ValidationError();
            verror.setErrorCode(HttpStatus.BAD_REQUEST);
            verror.setErrorDescription("Invalid Mapping between System, Container and Object");
            throw verror;
        }
        return objectSchemaMap;
    }

    public void updateObjectAutoRegistrationStatus(final long objectSchemaMapId,
            final boolean allAttrPresent, final String storageTypeName) {
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final String time = sdf.format(timestamp);
        final ObjectSchemaMap schemaMap = this.schemaMapRepository.findOne(objectSchemaMapId);
        schemaMap.setIsRegistered(
                allAttrPresent ? ActiveEnumeration.YES.getFlag() : ActiveEnumeration.PENDING.getFlag());
        schemaMap.setUpdatedTimestamp(time);
        this.schemaMapRepository.save(schemaMap);
    }

    public StorageType getTypeFromObject(final ObjectSchemaMap object) {
        final long storageSystemId = object.getStorageSystemId();
        final StorageType type = this.systemUtil.getStorageType(storageSystemId);
        return type;
    }

    public List<CollectiveObjectSchemaMap> getPagedObjectSchemaMaps(final List<ObjectSchemaMap> objects,
            final Gson gson, final ObjectSchemaMapRepository schemaMapRepository, final Pageable pageable,
            final ObjectSchemaAttributeValueRepository objectAttributeRepository, final long systemId,
            final List<Long> clusters) {
        final List<CollectiveObjectSchemaMap> modifiedObjects = new ArrayList<CollectiveObjectSchemaMap>();
        objects.stream().forEach(object -> {
            final List<ObjectAttributeValue> attributeValues = objectAttributeRepository
                    .findByObjectIdAndIsActiveYN(object.getObjectId(),
                            ActiveEnumeration.YES.getFlag());
            final String objectSchema = object.getObjectSchemaInString();
            List<Schema> modifiedSchema = new ArrayList<Schema>();
            if (objectSchema != null && objectSchema.length() > 0) {
                modifiedSchema = gson.fromJson(objectSchema, new TypeToken<List<Schema>>() {
                }.getType());
            }
            final List<CollectiveObjectAttributeValue> collectiveObjectAttributeValues = new ArrayList<CollectiveObjectAttributeValue>();
            attributeValues.forEach(attr -> {
                final CollectiveObjectAttributeValue tempAttr = new CollectiveObjectAttributeValue();
                tempAttr.setStorageDsAttributeKeyId(attr.getStorageDsAttributeKeyId());
                tempAttr.setObjectAttributeValue(attr.getObjectAttributeValue());
                tempAttr.setObjectId(object.getObjectId());
                collectiveObjectAttributeValues.add(tempAttr);
            });
            final CollectiveObjectSchemaMap collectiveObjectSchemaMap = new CollectiveObjectSchemaMap(
                    object.getObjectId(), object.getObjectName(), object.getContainerName(),
                    systemId, clusters, object.getQuery(), modifiedSchema,
                    collectiveObjectAttributeValues,
                    object.getIsActiveYN(), object.getCreatedUserOnStore(), object.getCreatedTimestampOnStore());
            modifiedObjects.add(collectiveObjectSchemaMap);
        });

        return modifiedObjects;

    }
}
