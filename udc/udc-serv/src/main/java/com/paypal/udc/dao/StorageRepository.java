package com.paypal.udc.dao;

import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;
import com.paypal.udc.entity.Storage;


@Repository
public interface StorageRepository extends CrudRepository<Storage, Long> {

    public Storage findByStorageName(String storageName);
}
