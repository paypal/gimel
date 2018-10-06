package com.paypal.udc.dao.storagesystem;

import java.util.List;
import org.springframework.data.repository.CrudRepository;
import com.paypal.udc.entity.storagesystem.StorageSystemContainer;


public interface StorageSystemContainerRepository extends CrudRepository<StorageSystemContainer, Long> {

    public List<StorageSystemContainer> findByStorageSystemId(long storageSystemId);

    public List<StorageSystemContainer> findByClusterId(long clusterId);

}
