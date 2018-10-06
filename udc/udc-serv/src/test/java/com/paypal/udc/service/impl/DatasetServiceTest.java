package com.paypal.udc.service.impl;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.ArrayList;
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
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.test.context.junit4.SpringRunner;
import com.paypal.udc.cache.DatasetCache;
import com.paypal.udc.cache.ObjectSchemaMapCache;
import com.paypal.udc.config.UDCInterceptorConfig;
import com.paypal.udc.dao.ClusterRepository;
import com.paypal.udc.dao.dataset.DatasetChangeLogRegisteredRepository;
import com.paypal.udc.dao.dataset.DatasetRepository;
import com.paypal.udc.dao.dataset.DatasetStorageSystemRepository;
import com.paypal.udc.dao.objectschema.ObjectSchemaAttributeValueRepository;
import com.paypal.udc.dao.objectschema.ObjectSchemaMapRepository;
//import com.paypal.gimel.dao.objectschema.ObjectSchemaRegistrationMapRepository;
import com.paypal.udc.dao.storagesystem.StorageSystemAttributeValueRepository;
import com.paypal.udc.dao.storagesystem.StorageSystemRepository;
import com.paypal.udc.dao.storagetype.StorageTypeRepository;
import com.paypal.udc.entity.Cluster;
import com.paypal.udc.entity.dataset.Dataset;
import com.paypal.udc.entity.dataset.DatasetChangeLogRegistered;
import com.paypal.udc.entity.storagesystem.StorageSystem;
import com.paypal.udc.entity.storagetype.StorageType;
import com.paypal.udc.interceptor.UDCInterceptor;
import com.paypal.udc.util.ClusterUtil;
import com.paypal.udc.util.DatasetUtil;
import com.paypal.udc.util.ObjectSchemaMapUtil;
import com.paypal.udc.util.StorageSystemUtil;
import com.paypal.udc.util.StorageTypeUtil;
import com.paypal.udc.util.UserUtil;
import com.paypal.udc.validator.dataset.DatasetAliasValidator;
import com.paypal.udc.validator.dataset.DatasetDescValidator;
import com.paypal.udc.validator.dataset.DatasetNameValidator;


@RunWith(SpringRunner.class)
public class DatasetServiceTest {

    @MockBean
    private UDCInterceptor udcInterceptor;

    @MockBean
    private UDCInterceptorConfig udcInterceptorConfig;

    @Mock
    private StorageSystemUtil systemUtil;

    @Mock
    private DatasetRepository dataSetRepository;

    @Mock
    private UserUtil userUtil;

    @Mock
    private ClusterUtil clusterUtil;

    @Mock
    private StorageSystemAttributeValueRepository systemAttributeValueRepository;

    @Mock
    private DatasetStorageSystemRepository datasetStorageSystemRepository;

    @Mock
    private StorageSystemRepository storageSystemRepository;

    @Mock
    private DatasetUtil dataSetUtil;

    @Mock
    private StorageTypeUtil storageTypeUtil;

    @Mock
    private StorageTypeRepository storageTypeRepository;

    @Mock
    private DatasetCache dataSetCache;

    @Mock
    private DatasetNameValidator s1;

    @Mock
    private DatasetDescValidator s2;

    @Mock
    private DatasetAliasValidator s4;

    @Mock
    private DatasetChangeLogRegisteredRepository changeLogRegisteredRepository;

    @Mock
    private ObjectSchemaMapRepository objectSchemaMapRepository;

    @Mock
    private ObjectSchemaMapCache objectSchemaMapCache;

    @Mock
    private ObjectSchemaAttributeValueRepository objectSchemaAttributeRepository;

    // @Mock
    // private ObjectSchemaRegistrationMapRepository objectSchemaClusterRepository;

    @Mock
    private ClusterRepository clusterRepository;

    @Mock
    private ObjectSchemaMapUtil schemaMapUtil;

    @InjectMocks
    private DatasetService datasetService;

    private String datasetSubstring;
    private String storageTypeName;
    private StorageType storageType;
    private StorageSystem storageSystem;
    private List<StorageSystem> storageSystems;
    private String storageSystemName;
    private Pageable pageable;
    private Page<Dataset> pageDataset;
    private Long dataSetId = 3L;
    private Dataset dataSet = new Dataset();
    private Map<Long, StorageSystem> systemMappings = new HashMap<Long, StorageSystem>();
    private List<Cluster> clusterList = new ArrayList<Cluster>();
    private Cluster cluster = new Cluster();
    private Long storageClusterId;
    private DatasetChangeLogRegistered changeLog;
    private List<DatasetChangeLogRegistered> changeLogs;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        this.datasetSubstring = "dataset";
        this.storageTypeName = "All";
        this.storageType = new StorageType();
        this.storageType.setStorageTypeId(0L);
        this.storageSystem = new StorageSystem();
        this.storageSystem.setStorageSystemId(0L);
        this.storageSystems = Arrays.asList(this.storageSystem);
        this.storageSystemName = "All";
        this.cluster.setClusterId(5L);
        this.clusterList.add(this.cluster);
        this.dataSet.setStorageSystemId(4L);
        this.dataSet.setStorageSystemName(this.storageSystemName);
        this.systemMappings.put(4L, this.storageSystem);
        this.storageClusterId = 6L;
        this.changeLog = new DatasetChangeLogRegistered();
        this.changeLogs = Arrays.asList(this.changeLog);
    }

    @Test
    public void verifyValidGetAllDatasetsByTypeAndSystem() throws Exception {
        when(this.dataSetUtil.getDatasetsForAllTypesAndSystems(this.datasetSubstring, this.pageable))
                .thenReturn(this.pageDataset);

        final Page<Dataset> result = this.datasetService.getAllDatasetsByTypeAndSystem(this.datasetSubstring,
                this.storageTypeName, this.storageSystemName, this.pageable);
        assertEquals(this.pageDataset, result);

        verify(this.dataSetUtil).getDatasetsForAllTypesAndSystems(this.datasetSubstring, this.pageable);
    }

    @Test
    public void verifyValidGetAllDeletedDatasetsByTypeAndSystem() throws Exception {
        when(this.dataSetUtil.getDeletedDatasetsForAllTypesAndSystems(this.datasetSubstring, this.pageable))
                .thenReturn(this.pageDataset);

        final Page<Dataset> result = this.datasetService.getAllDeletedDatasetsByTypeAndSystem(this.datasetSubstring,
                this.storageTypeName, this.storageSystemName, this.pageable);
        assertEquals(this.pageDataset, result);

        verify(this.dataSetUtil).getDeletedDatasetsForAllTypesAndSystems(this.datasetSubstring, this.pageable);
    }

    @Test
    public void verifyValidGetPendingDatasets() throws Exception {
        when(this.dataSetUtil.getPendingDatasetsForAllTypesAndSystems(this.datasetSubstring, this.pageable))
                .thenReturn(this.pageDataset);

        final Page<Dataset> result = this.datasetService.getPendingDatasets(this.datasetSubstring,
                this.storageTypeName, this.storageSystemName, this.pageable);
        assertEquals(this.pageDataset, result);

        verify(this.dataSetUtil).getPendingDatasetsForAllTypesAndSystems(this.datasetSubstring, this.pageable);
    }

    @Test
    public void verifyValidGetDataSetById() throws Exception {
        when(this.dataSetRepository.findOne(this.dataSetId)).thenReturn(this.dataSet);
        when(this.systemUtil.getStorageSystems()).thenReturn(this.systemMappings);
        when(this.clusterRepository.findAll()).thenReturn(this.clusterList);
        when(this.dataSetUtil.getDataSet(this.dataSet)).thenReturn(this.dataSet);

        final Dataset result = this.datasetService.getDataSetById(this.dataSetId);
        assertEquals(this.dataSet, result);

        verify(this.clusterRepository).findAll();
    }

    @Test
    public void verifyValidGetDatasetChangeLogs() throws Exception {
        when(this.changeLogRegisteredRepository.findChanges(this.storageClusterId))
                .thenReturn(this.changeLogs);

        final List<DatasetChangeLogRegistered> result = this.datasetService.getDatasetChangeLogs(this.storageClusterId);
        assertEquals(this.changeLogs, result);

        verify(this.changeLogRegisteredRepository).findChanges(this.storageClusterId);
    }

}
