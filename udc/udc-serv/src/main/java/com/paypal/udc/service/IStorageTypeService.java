package com.paypal.udc.service;

import java.util.List;
import com.paypal.udc.entity.storagetype.CollectiveStorageTypeAttributeKey;
import com.paypal.udc.entity.storagetype.StorageType;
import com.paypal.udc.entity.storagetype.StorageTypeAttributeKey;
import com.paypal.udc.exception.ValidationError;


public interface IStorageTypeService {

    List<StorageType> getAllStorageTypes();

    StorageType getStorageTypeById(long storageTypeId);

    StorageType addStorageType(StorageType storageType) throws ValidationError;

    StorageType updateStorageType(StorageType storageType) throws ValidationError;

    StorageType deleteStorageType(long storageTypeId) throws ValidationError;

    List<StorageType> getStorageTypeByStorageCategory(long storageId);

    List<StorageType> getStorageTypeByStorageCategoryName(String storageName);

    List<StorageTypeAttributeKey> getStorageAttributeKeys(final long storageTypeId, final String isStorageSystemLevel);

    StorageTypeAttributeKey updateStorageTypeAttributeKeys(StorageTypeAttributeKey attributeKey);

    StorageTypeAttributeKey insertStorageTypeAttributeKey(CollectiveStorageTypeAttributeKey attributeKey)
            throws ValidationError;

    void deleteStorageAttributeKey(long storageTypeId, String storageAttributeKeys);

    StorageType enableStorageType(long id) throws ValidationError;

    List<StorageTypeAttributeKey> getAllStorageAttributeKeys(long storageTypeId);

}
