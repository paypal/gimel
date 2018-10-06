package com.paypal.udc.service.impl;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.Arrays;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;
import com.paypal.udc.cache.StorageCache;
import com.paypal.udc.config.UDCInterceptorConfig;
import com.paypal.udc.dao.StorageRepository;
import com.paypal.udc.entity.Storage;
import com.paypal.udc.interceptor.UDCInterceptor;
import com.paypal.udc.util.UserUtil;
import com.paypal.udc.util.enumeration.ActiveEnumeration;
import com.paypal.udc.validator.storage.StorageDescValidator;
import com.paypal.udc.validator.storage.StorageNameValidator;


@RunWith(SpringRunner.class)
public class StorageServiceTest {

    @MockBean
    private UDCInterceptor udcInterceptor;

    @MockBean
    private UDCInterceptorConfig udcInterceptorConfig;

    @MockBean
    private UserUtil userUtil;

    @Mock
    private StorageRepository storageRepository;

    @Mock
    private StorageDescValidator s2;

    @Mock
    private StorageNameValidator s1;

    @Mock
    private StorageCache storageCache;

    @InjectMocks
    private StorageService storageService;

    private Storage storage;
    private Long storageId;
    private String storageName;
    private List<Storage> storageList;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        this.storageId = 1L;
        this.storageName = "storageName";
        this.storage = new Storage(this.storageId, this.storageName, "storageDescription", "crUser", "crTime",
                "updUser", "updTime");
        this.storageList = Arrays.asList(this.storage);
    }

    @Test
    public void verifyValidGetAllStorages() throws Exception {
        when(this.storageRepository.findAll()).thenReturn(this.storageList);

        final List<Storage> result = this.storageService.getAllStorages();
        assertEquals(this.storageList.size(), result.size());

        verify(this.storageRepository).findAll();
    }

    @Test
    public void verifyValidGetStorageById() throws Exception {
        when(this.storageRepository.findOne(this.storageId)).thenReturn(this.storage);

        final Storage result = this.storageService.getStorageById(this.storageId);
        assertEquals(this.storage, result);

        verify(this.storageRepository).findOne(this.storageId);
    }

    @Test
    public void verifyValidGetStorageByName() throws Exception {
        when(this.storageRepository.findByStorageName(this.storageName)).thenReturn(this.storage);

        final Storage result = this.storageService.getStorageByName(this.storageName);
        assertEquals(this.storage, result);

        verify(this.storageRepository).findByStorageName(this.storageName);
    }

    @Test
    public void verifyValidAddStorage() throws Exception {
        when(this.storageRepository.save(this.storage)).thenReturn(this.storage);

        final Storage result = this.storageService.addStorage(this.storage);
        assertEquals(this.storage, result);

        verify(this.storageRepository).save(this.storage);
    }

    @Test
    public void verifyValidUpdateStorage() throws Exception {
        when(this.storageCache.getStorage(this.storageId)).thenReturn(this.storage);
        when(this.storageRepository.save(this.storage)).thenReturn(this.storage);

        final Storage result = this.storageService.updateStorage(this.storage);
        assertEquals(this.storage, result);

        verify(this.storageRepository).save(this.storage);
    }

    @Test
    public void verifyValidDeleteStorage() throws Exception {
        when(this.storageCache.getStorage(this.storageId)).thenReturn(this.storage);
        when(this.storageRepository.save(this.storage)).thenReturn(this.storage);

        final Storage result = this.storageService.deleteStorage(this.storageId);
        assertEquals(this.storage, result);
        assertEquals(ActiveEnumeration.NO.getFlag(), result.getIsActiveYN());

        verify(this.storageRepository).save(this.storage);
    }

    @Test
    public void verifyValidEnableStorage() throws Exception {
        when(this.storageCache.getStorage(this.storageId)).thenReturn(this.storage);
        when(this.storageRepository.save(this.storage)).thenReturn(this.storage);

        final Storage result = this.storageService.enableStorage(this.storageId);
        assertEquals(this.storage, result);
        assertEquals(ActiveEnumeration.YES.getFlag(), result.getIsActiveYN());

        verify(this.storageRepository).save(this.storage);
    }
}
