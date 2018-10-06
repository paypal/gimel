package com.paypal.udc.service;

import java.util.List;
import com.paypal.udc.entity.storagesystem.CollectiveStorageSystemContainerObject;
import com.paypal.udc.entity.storagesystem.StorageSystemContainer;
import com.paypal.udc.exception.ValidationError;


public interface IStorageSystemContainerService {

    List<CollectiveStorageSystemContainerObject> getAllStorageSystemContainers(final long clusterId);

    List<StorageSystemContainer> getStorageSystemContainersByStorageSystemId(long storageId);

    StorageSystemContainer getStorageSystemContainerById(long storageSystemContainerId);

    StorageSystemContainer addStorageSystemContainer(StorageSystemContainer storageSystemContainer)
            throws ValidationError;

    StorageSystemContainer updateStorageSystemContainer(StorageSystemContainer storageSystemContainer)
            throws ValidationError;

    StorageSystemContainer deleteStorageSystemContainer(long storageSystemContainerId) throws ValidationError;

}
