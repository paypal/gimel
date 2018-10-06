package com.paypal.udc.dao.storagesystem;

import java.util.List;
import org.springframework.data.repository.CrudRepository;
import com.paypal.udc.entity.storagesystem.StorageSystem;


public interface StorageSystemRepository extends CrudRepository<StorageSystem, Long> {

    public List<StorageSystem> findByStorageTypeId(long storageTypeId);

    public StorageSystem findByStorageSystemName(String storageSystemName);

    public List<StorageSystem> findByIsActiveYN(final String isActiveYN);

}
