package com.paypal.udc.dao.storagesystem;

import java.util.List;
import org.springframework.data.repository.CrudRepository;
import com.paypal.udc.entity.storagesystem.StorageSystemAttributeValue;


public interface StorageSystemAttributeValueRepository extends CrudRepository<StorageSystemAttributeValue, Long> {

    public List<StorageSystemAttributeValue> findByStorageSystemIdAndIsActiveYN(long storageSystemID,
            final String isActiveYN);

    public StorageSystemAttributeValue findByStorageSystemIdAndStorageDataSetAttributeKeyIdAndStorageSystemAttributeValue(
            long storageSystemId, long storageDataSetAttributeKeyId, String storageSystemAttributeValue);

    public StorageSystemAttributeValue findByStorageSystemIdAndStorageDataSetAttributeKeyId(long storageSystemId,
            long storageDataSetAttributeKeyId);

    public List<StorageSystemAttributeValue> findByStorageSystemIdIn(final List<Long> storageSystemIds);
}
