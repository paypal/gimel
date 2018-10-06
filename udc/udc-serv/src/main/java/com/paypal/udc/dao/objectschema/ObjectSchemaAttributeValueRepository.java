package com.paypal.udc.dao.objectschema;

import java.util.List;
import org.springframework.data.repository.CrudRepository;
import com.paypal.udc.entity.objectschema.ObjectAttributeValue;


public interface ObjectSchemaAttributeValueRepository
        extends CrudRepository<ObjectAttributeValue, Long> {

    ObjectAttributeValue findByObjectIdAndStorageDsAttributeKeyId(final long objectId,
            final long storagDsAttributeKeyId);

    List<ObjectAttributeValue> findByObjectIdAndIsActiveYN(final long objectId, final String isActiveYN);

    ObjectAttributeValue findByObjectIdAndStorageDsAttributeKeyIdAndObjectAttributeValue(
            final long objectId, final long storagDsAttributeKeyId, final String objectAttributeValue);

    ObjectAttributeValue findByStorageDsAttributeKeyIdAndObjectId(long storageDsAttributeKeyId, long objectId);
}
