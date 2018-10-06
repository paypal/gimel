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
import com.paypal.udc.cache.UserCache;
import com.paypal.udc.config.UDCInterceptorConfig;
import com.paypal.udc.dao.storagesystem.StorageSystemContainerRepository;
import com.paypal.udc.dao.storagetype.StorageTypeAttributeKeyRepository;
import com.paypal.udc.entity.storagesystem.StorageSystemContainer;
import com.paypal.udc.interceptor.UDCInterceptor;
import com.paypal.udc.util.StorageSystemUtil;
import com.paypal.udc.util.enumeration.ActiveEnumeration;


@RunWith(SpringRunner.class)
public class StorageSystemContainerServiceTest {

    @MockBean
    private UDCInterceptor udcInterceptor;

    @MockBean
    private UDCInterceptorConfig udcInterceptorConfig;

    @Mock
    private StorageSystemContainerRepository storageSystemContainerRepository;

    @Mock
    private StorageSystemUtil storageSystemUtil;

    @Mock
    private StorageTypeAttributeKeyRepository stakr;

    @Mock
    private UserCache userCache;

    @InjectMocks
    private StorageSystemContainerService storageSystemContainerService;

    private Long storageSystemContainerId;
    private StorageSystemContainer storageSystemContainer;
    private List<StorageSystemContainer> storageSystemContainersList;
    private Long storageSystemId;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        this.storageSystemContainerId = 0L;
        this.storageSystemContainer = new StorageSystemContainer();
        this.storageSystemContainer.setIsActiveYN(ActiveEnumeration.YES.getFlag());
        this.storageSystemContainersList = Arrays.asList(this.storageSystemContainer);
        this.storageSystemId = 1L;
    }

    @Test
    public void verifyValidGetStorageSystemContainerById() throws Exception {
        when(this.storageSystemContainerRepository.findOne(this.storageSystemContainerId))
                .thenReturn(this.storageSystemContainer);

        final StorageSystemContainer result = this.storageSystemContainerService
                .getStorageSystemContainerById(this.storageSystemContainerId);
        assertEquals(this.storageSystemContainer, result);

        verify(this.storageSystemContainerRepository).findOne(this.storageSystemContainerId);
    }

    @Test
    public void verifyValidGetStorageSystemContainersByStorageSystemId() throws Exception {
        when(this.storageSystemContainerRepository.findByStorageSystemId(this.storageSystemId))
                .thenReturn(this.storageSystemContainersList);

        final List<StorageSystemContainer> result = this.storageSystemContainerService
                .getStorageSystemContainersByStorageSystemId(this.storageSystemId);
        assertEquals(this.storageSystemContainersList.size(), result.size());

        verify(this.storageSystemContainerRepository).findByStorageSystemId(this.storageSystemId);
    }

    @Test
    public void verifyValidDeleteStorageSystemContainer() throws Exception {
        when(this.storageSystemContainerRepository.findOne(this.storageSystemContainerId))
                .thenReturn(this.storageSystemContainer);
        when(this.storageSystemContainerRepository.save(this.storageSystemContainer))
                .thenReturn(this.storageSystemContainer);

        final StorageSystemContainer result = this.storageSystemContainerService
                .deleteStorageSystemContainer(this.storageSystemContainerId);
        assertEquals(this.storageSystemContainer, result);

        verify(this.storageSystemContainerRepository).findOne(this.storageSystemContainerId);
    }

    @Test
    public void verifyValidAddStorageSystemContainer() throws Exception {
        when(this.storageSystemContainerRepository.save(this.storageSystemContainer))
                .thenReturn(this.storageSystemContainer);

        final StorageSystemContainer result = this.storageSystemContainerService
                .addStorageSystemContainer(this.storageSystemContainer);
        assertEquals(this.storageSystemContainer, result);

        verify(this.storageSystemContainerRepository).save(this.storageSystemContainer);
    }

    @Test
    public void verifyValidUpdateStorageSystemContainer() throws Exception {
        when(this.storageSystemContainerRepository.findOne(this.storageSystemContainerId))
                .thenReturn(this.storageSystemContainer);
        when(this.storageSystemContainerRepository.save(this.storageSystemContainer))
                .thenReturn(this.storageSystemContainer);

        final StorageSystemContainer result = this.storageSystemContainerService
                .updateStorageSystemContainer(this.storageSystemContainer);
        assertEquals(this.storageSystemContainer, result);

        verify(this.storageSystemContainerRepository).save(this.storageSystemContainer);
    }

}
