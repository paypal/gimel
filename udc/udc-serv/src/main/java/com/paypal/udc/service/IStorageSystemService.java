package com.paypal.udc.service;

import java.util.List;
import com.paypal.udc.entity.storagesystem.StorageSystem;
import com.paypal.udc.entity.storagesystem.StorageSystemAttributeValue;
import com.paypal.udc.exception.ValidationError;


public interface IStorageSystemService {

    List<StorageSystem> getAllStorageSystems();

    StorageSystem getStorageSystemById(long storageSystemId);

    StorageSystem addStorageSystem(StorageSystem storageSystem) throws ValidationError;

    StorageSystem updateStorageSystem(StorageSystem storageSystem) throws ValidationError;

    StorageSystem deleteStorageSystem(long storageSystemId) throws ValidationError;

    List<StorageSystem> getStorageSystemByStorageType(long storageTypeId);

    List<StorageSystemAttributeValue> getStorageSystemAttributes(Long storageSystemId);

    List<StorageSystemAttributeValue> getAttributeValuesByName(String storageSystemName);

    List<StorageSystem> getStorageSystemByType(String name) throws ValidationError;

    StorageSystem enableStorageSystem(long id) throws ValidationError;

}
