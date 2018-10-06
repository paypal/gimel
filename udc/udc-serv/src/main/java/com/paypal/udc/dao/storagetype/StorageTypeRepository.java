package com.paypal.udc.dao.storagetype;

import java.util.List;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;
import com.paypal.udc.entity.storagetype.StorageType;


@Repository
public interface StorageTypeRepository extends CrudRepository<StorageType, Long> {

    public List<StorageType> findByStorageId(long storageId);

    public StorageType findByStorageTypeName(final String storageTypeName);
}
