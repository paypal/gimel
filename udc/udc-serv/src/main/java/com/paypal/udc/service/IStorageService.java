package com.paypal.udc.service;

import java.util.List;
import com.paypal.udc.entity.Storage;
import com.paypal.udc.exception.ValidationError;


public interface IStorageService {

    List<Storage> getAllStorages();

    Storage getStorageById(long storageId);

    Storage addStorage(Storage storage) throws ValidationError;

    Storage updateStorage(Storage storage) throws ValidationError;

    Storage deleteStorage(long storageId) throws ValidationError;

    Storage getStorageByName(String name);

    Storage enableStorage(long id) throws ValidationError;
}
