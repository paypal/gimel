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
import com.paypal.udc.cache.StorageSystemCache;
import com.paypal.udc.cache.StorageTypeCache;
import com.paypal.udc.config.UDCInterceptorConfig;
import com.paypal.udc.dao.ClusterRepository;
import com.paypal.udc.dao.storagesystem.StorageSystemAttributeValueRepository;
import com.paypal.udc.dao.storagesystem.StorageSystemContainerRepository;
import com.paypal.udc.dao.storagesystem.StorageSystemRepository;
import com.paypal.udc.dao.storagetype.StorageTypeAttributeKeyRepository;
import com.paypal.udc.dao.storagetype.StorageTypeRepository;
import com.paypal.udc.entity.Zone;
import com.paypal.udc.entity.storagesystem.StorageSystem;
import com.paypal.udc.entity.storagesystem.StorageSystemAttributeValue;
import com.paypal.udc.entity.storagesystem.StorageSystemContainer;
import com.paypal.udc.entity.storagetype.StorageType;
import com.paypal.udc.interceptor.UDCInterceptor;
import com.paypal.udc.util.ClusterUtil;
import com.paypal.udc.util.StorageSystemUtil;
import com.paypal.udc.util.StorageTypeUtil;
import com.paypal.udc.util.UserUtil;
import com.paypal.udc.util.ZoneUtil;
import com.paypal.udc.util.enumeration.ActiveEnumeration;
import com.paypal.udc.validator.storagesystem.StorageSystemDescValidator;
import com.paypal.udc.validator.storagesystem.StorageSystemNameValidator;
import com.paypal.udc.validator.storagesystem.StorageSystemTypeIDValidator;


@RunWith(SpringRunner.class)
public class StorageSystemServiceTest {

    @MockBean
    private UDCInterceptor udcInterceptor;

    @MockBean
    private UDCInterceptorConfig udcInterceptorConfig;

    @Mock
    private StorageSystemRepository storageSystemRepository;

    @Mock
    private StorageSystemContainerRepository storageSystemContainerRepository;

    @Mock
    private StorageTypeCache storageTypeCache;

    @Mock
    private ClusterRepository clusterRepository;

    @Mock
    private ClusterUtil clusterUtil;

    @Mock
    private StorageTypeRepository storageTypeRepository;

    @Mock
    private StorageSystemCache storageSystemCache;

    @Mock
    private StorageSystemAttributeValueRepository systemAttributeValueRepository;

    @Mock
    private StorageTypeAttributeKeyRepository typeAttributeKeyRepository;

    @Mock
    private UserUtil userUtil;

    @Mock
    private ZoneUtil zoneUtil;

    @Mock
    private StorageSystemContainerRepository systemContainerRepository;

    @Mock
    private StorageTypeUtil storageTypeUtil;

    @Mock
    private StorageSystemUtil storageSystemUtil;

    @Mock
    private StorageSystemTypeIDValidator s3;

    @Mock
    private StorageSystemNameValidator s1;

    @Mock
    private StorageSystemDescValidator s2;

    @Mock
    private StorageSystemAttributeValueRepository ssavr;

    @InjectMocks
    private StorageSystemService storageSystemService;

    @Mock
    private StorageSystem insStorageSystem;

    private Long storageSystemId;
    private Long clusterId;
    private Long zoneId;
    private String storageSystemName;
    private Long storageTypeId;
    private StorageSystem storageSystem;
    private List<StorageSystem> storageSystemsList;
    private StorageSystemAttributeValue attributeValue;
    private List<StorageSystemAttributeValue> attributeValuesList;
    private StorageType storageType;
    private Map<Long, StorageType> storageTypes;
    private Zone zone;
    private Map<Long, Zone> zones;
    private StorageSystemContainer ssc;
    private List<StorageSystemContainer> sscs;
    private String storageTypeName;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        this.storageSystemId = 0L;
        this.storageSystemName = "storageSystemName";
        this.storageTypeId = 0L;
        this.clusterId = 0L;
        this.zoneId = 0L;
        this.storageSystem = new StorageSystem(this.storageSystemId, this.storageSystemName, "storageSystemDescription",
                "crUser", "crTime", "updUser", "updTime", this.storageTypeId, this.clusterId, this.zoneId, "Y", "Y");
        this.storageSystem.setIsActiveYN(ActiveEnumeration.YES.getFlag());
        this.storageSystemsList = Arrays.asList(this.storageSystem);
        this.attributeValue = new StorageSystemAttributeValue();
        this.attributeValue.setStorageDataSetAttributeKeyId(4L);
        this.attributeValuesList = Arrays.asList(this.attributeValue);
        this.storageType = new StorageType("storageTypeName", "storageTypeDescription", "crUser", "crTime", "updUser",
                "updTime", 0L);
        this.zone = new Zone("zoneName", "zoneDescription", "isActiveYN", "createdUser", "createdTimestamp",
                "updatedUser", "updatedTimestamp");
        this.ssc = new StorageSystemContainer();
        this.sscs = Arrays.asList(this.ssc);
        this.storageTypeName = "All";
        this.zones = new HashMap<Long, Zone>();
        this.zones.put(0L, this.zone);
    }

    @Test
    public void verifyValidGetStorageSystemByStorageType() throws Exception {
        when(this.storageSystemRepository.findByStorageTypeId(this.storageTypeId)).thenReturn(this.storageSystemsList);

        final List<StorageSystem> result = this.storageSystemService
                .getStorageSystemByStorageType(this.storageSystemId);
        assertEquals(this.storageSystemsList, result);

        verify(this.storageSystemRepository).findByStorageTypeId(this.storageTypeId);
    }

    @Test
    public void verifyValidGetStorageSystemAttributes() throws Exception {
        when(this.storageSystemUtil.getAttributes(this.storageSystemId)).thenReturn(this.attributeValuesList);

        final List<StorageSystemAttributeValue> result = this.storageSystemService
                .getStorageSystemAttributes(this.storageSystemId);
        assertEquals(this.attributeValuesList, result);

        verify(this.storageSystemUtil).getAttributes(this.storageSystemId);
    }

    @Test
    public void verifyValidDeleteStorageSystem() throws Exception {
        when(this.storageSystemCache.getStorageSystem(this.storageSystemId)).thenReturn(this.storageSystem);
        when(this.ssavr.findByStorageSystemIdAndIsActiveYN(this.storageSystemId, ActiveEnumeration.YES.getFlag()))
                .thenReturn(this.attributeValuesList);
        when(this.storageSystemRepository.save(this.storageSystem)).thenReturn(this.storageSystem);
        when(this.ssavr.save(this.attributeValuesList)).thenReturn(this.attributeValuesList);
        when(this.storageSystemContainerRepository.findByStorageSystemId(this.storageSystemId)).thenReturn(this.sscs);

        final StorageSystem result = this.storageSystemService.deleteStorageSystem(this.storageSystemId);
        assertEquals(this.storageSystem, result);

        verify(this.storageSystemCache).getStorageSystem(this.storageSystemId);
    }

    @Test
    public void verifyValidGetStorageSystemByType() throws Exception {
        when(this.storageSystemRepository.findAll()).thenReturn(this.storageSystemsList);
        when(this.zoneUtil.getZones()).thenReturn(this.zones);
        final List<StorageSystem> result = this.storageSystemService.getStorageSystemByType(this.storageTypeName);
        assertEquals(this.storageSystemsList, result);

        verify(this.storageSystemRepository).findAll();
    }

    @Test
    public void verifyValidEnableStorageSystem() throws Exception {
        when(this.storageSystemCache.getStorageSystem(this.storageSystemId)).thenReturn(this.storageSystem);
        when(this.storageSystemRepository.save(this.storageSystem)).thenReturn(this.storageSystem);
        when(this.ssavr.save(this.attributeValuesList)).thenReturn(this.attributeValuesList);
        when(this.ssavr.findByStorageSystemIdAndIsActiveYN(this.storageSystemId, ActiveEnumeration.NO.getFlag()))
                .thenReturn(this.attributeValuesList);
        when(this.ssavr.save(this.attributeValuesList)).thenReturn(this.attributeValuesList);

        final StorageSystem result = this.storageSystemService.enableStorageSystem(this.storageSystemId);
        assertEquals(this.storageSystem, result);

        verify(this.storageSystemCache).getStorageSystem(this.storageSystemId);
    }
}
