package com.paypal.udc.controller;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;
import com.google.gson.Gson;
import com.paypal.udc.cache.DatasetCache;
import com.paypal.udc.config.UDCInterceptorConfig;
import com.paypal.udc.entity.dataset.Dataset;
import com.paypal.udc.entity.objectschema.CollectiveObjectAttributeValue;
import com.paypal.udc.entity.objectschema.CollectiveObjectSchemaMap;
import com.paypal.udc.entity.objectschema.ObjectSchemaMap;
import com.paypal.udc.entity.objectschema.Schema;
import com.paypal.udc.interceptor.UDCInterceptor;
import com.paypal.udc.service.IObjectSchemaMapService;


@RunWith(SpringRunner.class)
@WebMvcTest(ObjectSchemaMapController.class)
public class ObjectSchemaMapControllerTest {

    private MockMvc mockMvc;

    @MockBean
    private IObjectSchemaMapService objectSchemaMapService;

    @MockBean
    private DatasetCache dataSetCache;

    @MockBean
    private UDCInterceptor udcInterceptor;

    @MockBean
    private UDCInterceptorConfig udcInterceptorConfig;

    @Autowired
    private WebApplicationContext webApplicationContext;

    final Gson gson = new Gson();

    private String systemName, containerName, objectName;
    private Dataset dataset;
    private Long storageDataSetId;
    private String storageDataSetName;
    private List<Dataset> topicsList;
    private Long systemId;
    private CollectiveObjectSchemaMap schema;
    private List<CollectiveObjectSchemaMap> schemaList;
    private Long topicId;
    private ObjectSchemaMap topic;
    private Long storageSystemId;
    private List<String> objectNamesList;
    private List<String> containersList;
    private List<ObjectSchemaMap> schemaMapList;
    private String jsonObjectSchema;
    private Long objectId;
    private Page<CollectiveObjectSchemaMap> pages;
    private Pageable pageable;
    private int page;
    private int size;

    class AnyObjectSchemaMap extends ArgumentMatcher<ObjectSchemaMap> {
        @Override
        public boolean matches(final Object object) {
            return object instanceof ObjectSchemaMap;
        }
    }

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        this.mockMvc = MockMvcBuilders.webAppContextSetup(this.webApplicationContext).build();

        this.systemName = "systemName";
        this.containerName = "containerName";
        this.objectName = "objectName";
        this.storageDataSetId = 0L;
        this.systemId = 2L;
        this.storageDataSetName = "storageDataSetName";
        this.dataset = new Dataset(this.storageDataSetName, this.systemId, "storageDataSetAliasName",
                "storageContainerName", "storageDatabaseName", "storageDataSetDescription", "Y", "crUser", "crTime",
                "updUser", "updTime", 1L, "isAutoRegistered");
        this.topicsList = Arrays.asList(this.dataset);
        this.schema = new CollectiveObjectSchemaMap(3L, this.objectName, this.containerName, 2L,
                Collections.<Long> emptyList(), "query", Collections.<Schema> emptyList(),
                Collections.<CollectiveObjectAttributeValue> emptyList(), "Y", "", "");
        this.schemaList = Arrays.asList(this.schema);
        this.page = 0;
        this.size = 3;

        this.pageable = new PageRequest(this.page, this.size);
        this.pages = new PageImpl<CollectiveObjectSchemaMap>(
                this.schemaList, this.pageable, 1);
        this.topicId = 5L;
        this.topic = new ObjectSchemaMap();
        this.topic.setObjectName(this.objectName);
        this.storageSystemId = 6L;
        this.objectNamesList = Arrays.asList(this.objectName);
        this.containersList = Arrays.asList(this.containerName);
        this.schemaMapList = Arrays.asList(this.topic);
        this.objectId = 7L;
        this.jsonObjectSchema = "{ " +
                "\"clusters\": [ " +
                "0 " +
                "], " +
                "\"containerName\": \"string\", " +
                "\"createdTimestamp\": \"string\", " +
                "\"createdUser\": \"string\", " +
                "\"isActiveYN\": \"string\", " +
                "\"isSelfDiscovered\": \"string\", " +
                "\"objectAttributes\": [ " +
                "{ " +
                "\"createdTimestamp\": \"string\", " +
                "\"createdUser\": \"string\", " +
                "\"isActiveYN\": \"string\", " +
                "\"isCustomized\": \"string\", " +
                "\"objectAttributeValue\": \"string\", " +
                "\"objectAttributeValueId\": 0, " +
                "\"objectId\": 0, " +
                "\"storageDsAttributeKeyId\": 0, " +
                "\"storageDsAttributeKeyName\": \"string\", " +
                "\"updatedTimestamp\": \"string\", " +
                "\"updatedUser\": \"string\" " +
                "} " +
                "], " +
                "\"objectId\": 0, " +
                "\"objectName\": \"string\", " +
                "\"objectSchema\": [ " +
                "{ " +
                "\"columnFamily\": \"string\", " +
                "\"columnName\": \"string\", " +
                "\"columnType\": \"string\" " +
                "} " +
                "], " +
                "\"objectSchemaInString\": \"string\", " +
                "\"pendingAttributes\": [ " +
                "{ " +
                "\"createdTimestamp\": \"string\", " +
                "\"createdUser\": \"string\", " +
                "\"isActiveYN\": \"string\", " +
                "\"isCustomized\": \"string\", " +
                "\"objectAttributeValue\": \"string\", " +
                "\"objectAttributeValueId\": 0, " +
                "\"objectId\": 0, " +
                "\"storageDsAttributeKeyId\": 0, " +
                "\"storageDsAttributeKeyName\": \"string\", " +
                "\"updatedTimestamp\": \"string\", " +
                "\"updatedUser\": \"string\" " +
                "} " +
                "], " +
                "\"query\": \"string\", " +
                "\"storageSystemId\": 0, " +
                "\"updatedTimestamp\": \"string\", " +
                "\"updatedUser\": \"string\" " +
                "} ";
    }

    @Test
    public void verifyValidGetDatasetBySystemContainerAndObject() throws Exception {
        when(this.objectSchemaMapService.getDatasetBySystemContainerAndObject(this.systemName, this.containerName,
                this.objectName))
                        .thenReturn(this.topicsList);

        this.mockMvc
                .perform(get("/objectschema/schema/{systemName:.+}/{containerName:.+}/{objectName:.+}", this.systemName,
                        this.containerName, this.objectName)
                                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(this.topicsList.size())));

        verify(this.objectSchemaMapService).getDatasetBySystemContainerAndObject(this.systemName, this.containerName,
                this.objectName);
    }

    @Test
    public void verifyValidGetSchema() throws Exception {
        when(this.objectSchemaMapService.getSchemaBySystemContainerAndObject(this.systemId, this.containerName,
                this.objectName))
                        .thenReturn(this.schemaList);

        this.mockMvc
                .perform(get("/objectschema/objectSchema/{systemId}/{containerName:.+}/{objectName:.+}", this.systemId,
                        this.containerName, this.objectName)
                                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(this.schemaList.size())));

        verify(this.objectSchemaMapService).getSchemaBySystemContainerAndObject(this.systemId, this.containerName,
                this.objectName);
    }

    @Test
    public void verifyValidGetObjectById() throws Exception {
        when(this.objectSchemaMapService.getDatasetById(this.topicId))
                .thenReturn(this.topic);

        this.mockMvc.perform(get("/objectschema/schema/{id}", this.topicId)
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.objectName").value(this.objectName));

        verify(this.objectSchemaMapService).getDatasetById(this.topicId);
    }

    @Test
    public void verifyValidGetObjectsByContainer() throws Exception {
        when(this.objectSchemaMapService.getDistinctObjectNames(this.containerName, this.storageSystemId))
                .thenReturn(this.objectNamesList);

        this.mockMvc
                .perform(get("/objectschema/objectnames/{containerName:.+}/{storageSystemId}", this.containerName,
                        this.storageSystemId)
                                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(this.objectNamesList.size())));

        verify(this.objectSchemaMapService).getDistinctObjectNames(this.containerName, this.storageSystemId);
    }

    @Test
    public void verifyValidGetAllContainersByStorageSystemId() throws Exception {
        when(this.objectSchemaMapService.getDistinctContainerNamesByStorageSystemId(this.storageSystemId))
                .thenReturn(this.containersList);

        this.mockMvc
                .perform(get("/objectschema/containers/{storageSystemId}", this.storageSystemId)
                        .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(this.containersList.size())));

        verify(this.objectSchemaMapService).getDistinctContainerNamesByStorageSystemId(this.storageSystemId);
    }

    @Test
    public void verifyValidGetAllContainers() throws Exception {
        when(this.objectSchemaMapService.getDistinctContainerNames())
                .thenReturn(this.containersList);

        this.mockMvc
                .perform(get("/objectschema/containers")
                        .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(this.containersList.size())));

        verify(this.objectSchemaMapService).getDistinctContainerNames();
    }

    // @Test
    // public void verifyValidGetAllObjectSchemas() throws Exception {
    // when(this.objectSchemaMapService.getAllObjectMappings(this.storageSystemId))
    // .thenReturn(this.schemaList);
    //
    // this.mockMvc
    // .perform(get("/objectschema/schemas/{systemId}", this.storageSystemId)
    // .accept(MediaType.APPLICATION_JSON_UTF8))
    // .andExpect(status().isOk())
    // .andExpect(jsonPath("$", hasSize(this.schemaList.size())));
    //
    // verify(this.objectSchemaMapService).getAllObjectMappings(this.storageSystemId);
    // }

    @Test
    public void verifyValidGetObjectSchemaMapsBySystemIds() throws Exception {
        when(this.objectSchemaMapService.getObjectSchemaMapsBySystemIds(this.storageSystemId))
                .thenReturn(this.schemaMapList);

        this.mockMvc
                .perform(get("/objectschema/schemasWithClusters/{systemId}", this.storageSystemId)
                        .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(this.schemaMapList.size())));

        verify(this.objectSchemaMapService).getObjectSchemaMapsBySystemIds(this.storageSystemId);
    }

    @Test
    public void verifyValidGetUnRegisteredObjects() throws Exception {

        when(this.objectSchemaMapService.getPagedUnRegisteredObjects(this.systemId, this.pageable))
                .thenReturn(this.pages);

        this.mockMvc
                .perform(get("/objectschema/unregisteredSchemas/{systemId}", 2L)
                        .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$").exists());

        verify(this.objectSchemaMapService).getPagedUnRegisteredObjects(2L, this.pageable);
    }

    @Test
    public void verifyValidAddObjectSchema() throws Exception {
        when(this.objectSchemaMapService.addObjectSchema(argThat(new AnyObjectSchemaMap())))
                .thenReturn(this.topic);

        this.mockMvc.perform(post("/objectschema/schema")
                .contentType(MediaType.APPLICATION_JSON)
                .content(this.jsonObjectSchema)
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$").exists());

        verify(this.objectSchemaMapService).addObjectSchema(argThat(new AnyObjectSchemaMap()));
    }

    @Test
    public void verifyValidUpdateSchemaForObject() throws Exception {
        when(this.objectSchemaMapService.updateObjectSchemaMap(argThat(new AnyObjectSchemaMap())))
                .thenReturn(this.topic);

        this.mockMvc.perform(put("/objectschema/schema")
                .contentType(MediaType.APPLICATION_JSON)
                .content(this.jsonObjectSchema)
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$").exists());

        verify(this.objectSchemaMapService).updateObjectSchemaMap(argThat(new AnyObjectSchemaMap()));
    }

    @Test
    public void verifyValidDeactivateObjectAndDataset() throws Exception {
        final String expectedResult = "Deactivated the Object";

        doNothing().when(this.objectSchemaMapService).deActivateObjectAndDataset(this.objectId);

        this.mockMvc.perform(put("/objectschema/deactivate/{objectId}", this.objectId)
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString(expectedResult)));

        verify(this.objectSchemaMapService).deActivateObjectAndDataset(this.objectId);
    }
}
