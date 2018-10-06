package com.paypal.udc.dao.storagetype;

import java.util.List;
import org.springframework.data.repository.CrudRepository;
import com.paypal.udc.entity.storagetype.StorageTypeAttributeKey;


public interface StorageTypeAttributeKeyRepository extends CrudRepository<StorageTypeAttributeKey, Long> {

    public List<StorageTypeAttributeKey> findByStorageTypeIdAndIsActiveYN(long storageTypeId, String isActiveYN);

    public List<StorageTypeAttributeKey> findByStorageTypeIdAndIsStorageSystemLevelAndIsActiveYN(long storageTypeId,
            String isStorageSystemLevel, String isActiveYN);

    public StorageTypeAttributeKey findByStorageDsAttributeKeyName(String storageDsAttributeKeyName);

    public List<StorageTypeAttributeKey> findByStorageTypeId(long storageId);

    public StorageTypeAttributeKey findByStorageDsAttributeKeyNameAndStorageTypeId(
            final String storageDsAttributeKeyName, final long storageTypeId);
}
