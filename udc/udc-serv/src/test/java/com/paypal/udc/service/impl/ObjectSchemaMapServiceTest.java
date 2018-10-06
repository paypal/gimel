package com.paypal.udc.service.impl;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.test.context.junit4.SpringRunner;
import com.paypal.udc.cache.ObjectSchemaMapCache;
import com.paypal.udc.config.UDCInterceptorConfig;
import com.paypal.udc.dao.ClusterRepository;
import com.paypal.udc.dao.dataset.DatasetChangeLogRegisteredRepository;
import com.paypal.udc.dao.dataset.DatasetRepository;
import com.paypal.udc.dao.dataset.DatasetStorageSystemRepository;
import com.paypal.udc.dao.objectschema.ObjectSchemaAttributeValueRepository;
import com.paypal.udc.dao.objectschema.ObjectSchemaMapRepository;
import com.paypal.udc.dao.objectschema.PageableObjectSchemaMapRepository;
import com.paypal.udc.dao.storagesystem.StorageSystemRepository;
import com.paypal.udc.dao.storagetype.StorageTypeAttributeKeyRepository;
import com.paypal.udc.entity.Cluster;
import com.paypal.udc.entity.dataset.Dataset;
import com.paypal.udc.entity.objectschema.ObjectSchemaMap;
import com.paypal.udc.entity.storagesystem.StorageSystem;
import com.paypal.udc.interceptor.UDCInterceptor;
import com.paypal.udc.util.ClusterUtil;
import com.paypal.udc.util.DatasetUtil;
import com.paypal.udc.util.ObjectSchemaMapUtil;
import com.paypal.udc.util.StorageSystemUtil;
import com.paypal.udc.util.StorageTypeUtil;
import com.paypal.udc.util.UserUtil;


@RunWith(SpringRunner.class)
public class ObjectSchemaMapServiceTest {

    @MockBean
    private UDCInterceptor udcInterceptor;

    @MockBean
    private UDCInterceptorConfig udcInterceptorConfig;

    @Mock
    private ObjectSchemaMapRepository schemaMapRepository;

    @Mock
    private ObjectSchemaMapCache schemaMapCache;

    @Mock
    private DatasetChangeLogRegisteredRepository changeLogRegisteredRepository;

    @Mock
    private ObjectSchemaAttributeValueRepository objectAttributeRepository;

    @Mock
    private StorageSystemUtil storageSystemUtil;

    @Mock
    private StorageTypeUtil storageTypeUtil;

    @Mock
    private ObjectSchemaMapUtil schemaMapUtil;

    @Mock
    private StorageSystemRepository storageSystemRepository;

    @Mock
    private ClusterRepository clusterRepository;

    @Mock
    private DatasetStorageSystemRepository datasetSystemRepository;

    @Mock
    private DatasetRepository datasetRepository;

    @Mock
    private UserUtil userUtil;

    @Mock
    private ClusterUtil clusterUtil;

    @Mock
    private DatasetUtil datasetUtil;

    @Mock
    private PageableObjectSchemaMapRepository pageableSchemaMapRepository;

    @Mock
    private StorageTypeAttributeKeyRepository stakr;

    @InjectMocks
    private ObjectSchemaMapService objectSchemaMapService;

    private Long storageSystemId;
    private String containerName;
    private List<String> containerNames;
    private String systemName;
    private String objectName;
    private StorageSystem storageSystem;
    private Long objectId;
    private ObjectSchemaMap objectSchemaMap;
    private List<Dataset> datasets;
    private Pageable pageable;
    private Page<ObjectSchemaMap> page;
    private String storageSystemName;
    private Long clusterId;
    private Cluster cluster;
    private List<Cluster> clusterList;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        this.storageSystemId = 0L;
        this.containerName = "All";
        this.containerNames = Arrays.asList(this.containerName);
        this.systemName = "systemName";
        this.objectName = "objectName";
        this.storageSystem = new StorageSystem();
        this.storageSystem.setStorageSystemId(this.storageSystemId);
        this.objectId = 1L;
        this.objectSchemaMap = new ObjectSchemaMap();
        this.objectSchemaMap.setObjectId(this.objectId);
        this.storageSystemName = "All";
        this.cluster = new Cluster();
        this.clusterId = 2L;
        this.cluster.setClusterId(this.clusterId);
        this.clusterList = Arrays.asList(this.cluster);
    }

    @Test
    public void verifyValidGetDistinctContainerNamesByStorageSystemId() throws Exception {
        when(this.schemaMapRepository.findAllContainerNamesByStorageSystemId(this.storageSystemId))
                .thenReturn(this.containerNames);

        final List<String> result = this.objectSchemaMapService
                .getDistinctContainerNamesByStorageSystemId(this.storageSystemId);
        assertEquals(this.containerNames.size(), result.size());

        verify(this.schemaMapRepository).findAllContainerNamesByStorageSystemId(this.storageSystemId);
    }

    @Test
    public void verifyValidGetDistinctContainerNames() throws Exception {
        when(this.schemaMapRepository.findAllContainerNames())
                .thenReturn(this.containerNames);

        final List<String> result = this.objectSchemaMapService.getDistinctContainerNames();
        assertEquals(this.containerNames.size(), result.size());

        verify(this.schemaMapRepository).findAllContainerNames();
    }

    @Test
    public void verifyValidGetDistinctObjectNames() throws Exception {
        when(this.schemaMapRepository.findAllObjectNames(this.containerName, this.storageSystemId))
                .thenReturn(this.containerNames);

        final List<String> result = this.objectSchemaMapService.getDistinctObjectNames(this.containerName,
                this.storageSystemId);
        assertEquals(this.containerNames.size(), result.size());

        verify(this.schemaMapRepository).findAllObjectNames(this.containerName, this.storageSystemId);
    }

    @Test
    public void verifyValidGetDatasetBySystemContainerAndObject() throws Exception {
        when(this.storageSystemUtil.getStorageSystem(this.systemName)).thenReturn(this.storageSystem);
        when(this.schemaMapRepository.findByStorageSystemIdAndContainerNameAndObjectName(this.storageSystemId,
                this.containerName, this.objectName)).thenReturn(this.objectSchemaMap);
        when(this.datasetRepository.findByObjectSchemaMapId(this.objectId)).thenReturn(this.datasets);

        final List<Dataset> result = this.objectSchemaMapService.getDatasetBySystemContainerAndObject(this.systemName,
                this.containerName, this.objectName);
        assertEquals(this.datasets, result);

        verify(this.datasetRepository).findByObjectSchemaMapId(this.objectId);
    }

    @Test
    public void verifyValidGetObjectsByStorageSystemAndContainer() throws Exception {
        final List<Long> storageSystemIds = new ArrayList<Long>();
        when(this.clusterRepository.findAll()).thenReturn(this.clusterList);
        when(this.pageableSchemaMapRepository.findAll(this.pageable)).thenReturn(this.page);
        when(this.pageableSchemaMapRepository.findByContainerNameAndStorageSystemIdIn(storageSystemIds,
                this.containerName, this.pageable)).thenReturn(this.page);
        when(this.pageableSchemaMapRepository.findByStorageSystemIdIn(storageSystemIds, this.pageable))
                .thenReturn(this.page);

        final Page<ObjectSchemaMap> result = this.objectSchemaMapService
                .getObjectsByStorageSystemAndContainer(this.storageSystemName, this.containerName, this.pageable);
        assertEquals(this.page, result);

        verify(this.pageableSchemaMapRepository).findByStorageSystemIdIn(storageSystemIds, this.pageable);
    }

}
