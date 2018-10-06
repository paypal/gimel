package com.paypal.udc.service.impl;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;
import com.paypal.udc.cache.StorageCache;
import com.paypal.udc.cache.StorageTypeCache;
import com.paypal.udc.config.UDCInterceptorConfig;
import com.paypal.udc.dao.StorageRepository;
import com.paypal.udc.dao.storagetype.StorageTypeAttributeKeyRepository;
import com.paypal.udc.dao.storagetype.StorageTypeRepository;
import com.paypal.udc.entity.Storage;
import com.paypal.udc.entity.storagetype.StorageType;
import com.paypal.udc.interceptor.UDCInterceptor;
import com.paypal.udc.util.StorageTypeUtil;
import com.paypal.udc.util.enumeration.ActiveEnumeration;
import com.paypal.udc.validator.storagetype.StorageIDValidator;
import com.paypal.udc.validator.storagetype.StorageTypeDescValidator;
import com.paypal.udc.validator.storagetype.StorageTypeNameValidator;


@RunWith(SpringRunner.class)
public class StorageTypeServiceTest {

    @MockBean
    private UDCInterceptor udcInterceptor;

    @MockBean
    private UDCInterceptorConfig udcInterceptorConfig;

    @Mock
    private StorageTypeRepository storageTypeRepository;

    @Mock
    private StorageTypeAttributeKeyRepository storageAttributeRepository;

    @Mock
    private StorageCache storageCache;

    @Mock
    private StorageTypeUtil storageTypeUtil;

    @Mock
    private StorageIDValidator s3;

    @Mock
    private StorageTypeNameValidator s1;

    @Mock
    private StorageTypeDescValidator s2;

    @Mock
    private StorageTypeCache storageTypeCache;

    @Mock
    private StorageRepository storageRepository;

    @InjectMocks
    private StorageTypeService storageTypeService;

    @Mock
    private Map<Long, Storage> storageMap = new HashMap<Long, Storage>();
    private StorageType storageType;
    private List<StorageType> storageTypesList;
    private Storage storage;
    private Long storageTypeId;
    private Long storageId;
    private String storageName;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        this.storageType = new StorageType();
        this.storageType.setIsActiveYN(ActiveEnumeration.YES.getFlag());
        this.storageTypeId = 0L;
        this.storageId = 0L;
        this.storageType.setStorageId(this.storageId);
        this.storageTypesList = Arrays.asList(this.storageType);
        this.storage = new Storage();
        this.storageName = "All";
    }

    @Test
    public void verifyValidGetAllStorageTypes() throws Exception {
        when(this.storageTypeUtil.getStorages()).thenReturn(this.storageMap);
        when(this.storageTypeRepository.findAll()).thenReturn(this.storageTypesList);
        when(this.storageMap.get(this.storageTypeId)).thenReturn(this.storage);

        final List<StorageType> result = this.storageTypeService.getAllStorageTypes();
        assertEquals(this.storageTypesList.size(), result.size());

        verify(this.storageTypeRepository).findAll();
    }

    @Test
    public void verifyValidGetStorageTypeById() throws Exception {
        when(this.storageTypeUtil.getStorages()).thenReturn(this.storageMap);
        when(this.storageTypeRepository.findOne(this.storageId)).thenReturn(this.storageType);
        when(this.storageMap.get(0L)).thenReturn(this.storage);

        final StorageType result = this.storageTypeService.getStorageTypeById(this.storageTypeId);
        assertEquals(this.storageType, result);

        verify(this.storageTypeRepository).findOne(this.storageId);
    }

    @Test
    public void verifyValidGetStorageTypeByStorageCategory() throws Exception {
        when(this.storageTypeUtil.getStorages()).thenReturn(this.storageMap);
        when(this.storageTypeRepository.findByStorageId(this.storageId)).thenReturn(this.storageTypesList);
        when(this.storageMap.get(this.storageTypeId)).thenReturn(this.storage);

        final List<StorageType> result = this.storageTypeService.getStorageTypeByStorageCategory(this.storageId);
        assertEquals(this.storageTypesList.size(), result.size());

        verify(this.storageTypeRepository).findByStorageId(this.storageId);
    }

    @Test
    public void verifyValidGetStorageTypeByStorageCategoryName() throws Exception {
        when(this.storageTypeRepository.findAll()).thenReturn(this.storageTypesList);

        final List<StorageType> result = this.storageTypeService.getStorageTypeByStorageCategoryName(this.storageName);
        assertEquals(this.storageTypesList.size(), result.size());

        verify(this.storageTypeRepository).findAll();
    }

    @Test
    public void verifyValidEnableStorageType() throws Exception {
        when(this.storageTypeCache.getStorageType(this.storageTypeId)).thenReturn(this.storageType);
        when(this.storageTypeRepository.save(this.storageType)).thenReturn(this.storageType);

        final StorageType result = this.storageTypeService.enableStorageType(this.storageTypeId);
        assertEquals(this.storageType, result);

        verify(this.storageTypeRepository).save(this.storageType);
    }
}
