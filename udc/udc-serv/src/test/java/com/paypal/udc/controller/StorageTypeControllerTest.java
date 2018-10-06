package com.paypal.udc.controller;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import java.util.Arrays;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;
import com.google.gson.Gson;
import com.paypal.udc.cache.StorageTypeCache;
import com.paypal.udc.config.UDCInterceptorConfig;
import com.paypal.udc.entity.storagetype.CollectiveStorageTypeAttributeKey;
import com.paypal.udc.entity.storagetype.StorageType;
import com.paypal.udc.entity.storagetype.StorageTypeAttributeKey;
import com.paypal.udc.interceptor.UDCInterceptor;
import com.paypal.udc.service.IStorageTypeService;


@RunWith(SpringRunner.class)
@WebMvcTest(StorageTypeController.class)
public class StorageTypeControllerTest {

    private MockMvc mockMvc;

    @MockBean
    private IStorageTypeService storageTypeService;

    @MockBean
    private StorageTypeCache storageTypeCache;

    @MockBean
    private UDCInterceptor udcInterceptor;

    @MockBean
    private UDCInterceptorConfig udcInterceptorConfig;

    @Autowired
    private WebApplicationContext webApplicationContext;

    final Gson gson = new Gson();

    private Long storageTypeId;
    private StorageType storageType;
    private String storageTypeName;
    private StorageTypeAttributeKey storageTypeAttributeKey;
    private List<StorageTypeAttributeKey> storageAttributeKeys;
    private String isStorageSystemLevel;
    private List<StorageType> storageTypes;
    private String storageName;
    private String jsonStorageType;
    private String jsonStak;

    class AnyStorageType extends ArgumentMatcher<StorageType> {
        @Override
        public boolean matches(final Object object) {
            return object instanceof StorageType;
        }
    }

    class AnyCollectiveStorageTypeAttributeKey extends ArgumentMatcher<CollectiveStorageTypeAttributeKey> {
        @Override
        public boolean matches(final Object object) {
            return object instanceof CollectiveStorageTypeAttributeKey;
        }
    }

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        this.mockMvc = MockMvcBuilders.webAppContextSetup(this.webApplicationContext).build();

        this.storageTypeId = 0L;
        this.storageTypeName = "storageTypeName";
        this.storageType = new StorageType(this.storageTypeName, "storageTypeDescription", "crUser",
                "crTime", "updUser", "updTime", this.storageTypeId);
        this.isStorageSystemLevel = "isStorageSystemLevel";
        this.storageTypeAttributeKey = new StorageTypeAttributeKey("storageDsAttributeKeyName",
                "storageDsAttributeKeyDesc", "crUser", "crTime", "updUser", "updTime", this.storageTypeId, "Y",
                this.isStorageSystemLevel);
        this.storageAttributeKeys = Arrays.asList(this.storageTypeAttributeKey);
        this.storageTypes = Arrays.asList(this.storageType);
        this.storageName = "storageName";
        this.jsonStorageType = "{ " +
                "\"attributeKeys\": [ " +
                "{ " +
                "\"createdTimestamp\": \"crTime\", " +
                "\"createdUser\": \"crUser\", " +
                "\"isActiveYN\": \"Y\", " +
                "\"isStorageSystemLevel\": \"isStorageSystemLevel\", " +
                "\"storageDsAttributeKeyDesc\": \"storageDsAttributeKeyDesc\", " +
                "\"storageDsAttributeKeyId\": 0, " +
                "\"storageDsAttributeKeyName\": \"storageDsAttributeKeyName\", " +
                "\"storageTypeAttributeValue\": \"storageTypeAttributeValue\", " +
                "\"storageTypeId\": 0, " +
                "\"updatedTimestamp\": \"updTime\", " +
                "\"updatedUser\": \"updUser\" " +
                "} " +
                "], " +
                "\"createdUser\": \"crUser\", " +
                "\"isActiveYN\": \"Y\", " +
                "\"storage\": { " +
                "\"createdTimestamp\": \"crTime\", " +
                "\"createdUser\": \"crUser\", " +
                "\"isActiveYN\": \"Y\", " +
                "\"storageDescription\": \"storageDescription\", " +
                "\"storageId\": 0, " +
                "\"storageName\": \"storageName\", " +
                "\"updatedUser\": \"updUser\" " +
                "}, " +
                "\"storageId\": 0, " +
                "\"storageTypeDescription\": \"storageTypeDescription\", " +
                "\"storageTypeId\": 0, " +
                "\"storageTypeName\": \"storageTypeName\", " +
                "\"updatedUser\": \"updUser\" " +
                "}";
        this.jsonStak = "{ " +
                "\"createdUser\": \"crUser\", " +
                "\"isStorageSystemLevel\": \"isStorageSystemLevel\", " +
                "\"storageDsAttributeKeyDesc\": \"storageDsAttributeKeyDesc\", " +
                "\"storageDsAttributeKeyName\": \"storageDsAttributeKeyName\", " +
                "\"storageTypeId\": 0 " +
                "}";
    }

    @Test
    public void verifyValidGetStorageTypeById() throws Exception {
        when(this.storageTypeCache.getStorageType(this.storageTypeId))
                .thenReturn(this.storageType);

        this.mockMvc.perform(get("/storageType/storageType/{id}", this.storageTypeId)
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.storageTypeName").value(this.storageTypeName));

        verify(this.storageTypeCache).getStorageType(this.storageTypeId);
    }

    @Test
    public void verifyValidGetStorageAttributeKeysById() throws Exception {
        when(this.storageTypeService.getStorageAttributeKeys(this.storageTypeId, this.isStorageSystemLevel))
                .thenReturn(this.storageAttributeKeys);

        this.mockMvc
                .perform(get("/storageType/storageAttributeKey/{id}/{isStorageSystemLevel}", this.storageTypeId,
                        this.isStorageSystemLevel)
                                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(this.storageAttributeKeys.size())));

        verify(this.storageTypeService).getStorageAttributeKeys(this.storageTypeId, this.isStorageSystemLevel);
    }

    @Test
    public void verifyValidGetAllStorageAttributeKeysById() throws Exception {
        when(this.storageTypeService.getAllStorageAttributeKeys(this.storageTypeId))
                .thenReturn(this.storageAttributeKeys);

        this.mockMvc
                .perform(get("/storageType/storageAttributeKey/{id}", this.storageTypeId,
                        this.isStorageSystemLevel)
                                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(this.storageAttributeKeys.size())));

        verify(this.storageTypeService).getAllStorageAttributeKeys(this.storageTypeId);
    }

    @Test
    public void verifyValidGetStorageTypeByStorage() throws Exception {
        when(this.storageTypeService.getStorageTypeByStorageCategory(this.storageTypeId))
                .thenReturn(this.storageTypes);

        this.mockMvc
                .perform(get("/storageType/storageTypeByStorageId/{storageId}", this.storageTypeId)
                        .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(this.storageTypes.size())));

        verify(this.storageTypeService).getStorageTypeByStorageCategory(this.storageTypeId);
    }

    @Test
    public void verifyValidGetStorageTypeByStorageName() throws Exception {
        when(this.storageTypeService.getStorageTypeByStorageCategoryName(this.storageName))
                .thenReturn(this.storageTypes);

        this.mockMvc
                .perform(get("/storageType/storageTypeByStorageName/{storageName:.+}", this.storageName)
                        .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(this.storageTypes.size())));

        verify(this.storageTypeService).getStorageTypeByStorageCategoryName(this.storageName);
    }

    @Test
    public void verifyValidGetAllStorageTypes() throws Exception {
        when(this.storageTypeService.getAllStorageTypes())
                .thenReturn(this.storageTypes);

        this.mockMvc
                .perform(get("/storageType/storageTypes")
                        .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(this.storageTypes.size())));

        verify(this.storageTypeService).getAllStorageTypes();
    }

    @Test
    public void verifyValidAddStorageType() throws Exception {
        when(this.storageTypeService.addStorageType(argThat(new AnyStorageType())))
                .thenReturn(this.storageType);

        this.mockMvc.perform(post("/storageType/storageType")
                .contentType(MediaType.APPLICATION_JSON)
                .content(this.jsonStorageType)
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.storageTypeName").exists())
                .andExpect(jsonPath("$.storageTypeName").value(this.storageTypeName));

        verify(this.storageTypeService).addStorageType(argThat(new AnyStorageType()));
    }

    @Test
    public void verifyValidUpdateStorageType() throws Exception {
        when(this.storageTypeService.updateStorageType(argThat(new AnyStorageType())))
                .thenReturn(this.storageType);

        this.mockMvc.perform(put("/storageType/storageType")
                .contentType(MediaType.APPLICATION_JSON)
                .content(this.jsonStorageType)
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.storageTypeName").exists())
                .andExpect(jsonPath("$.storageTypeName").value(this.storageTypeName));

        verify(this.storageTypeService).updateStorageType(argThat(new AnyStorageType()));
    }

    @Test
    public void verifyValidDeleteStorageType() throws Exception {
        final String expectedResult = "Deactivated " + this.storageTypeId;

        when(this.storageTypeService.deleteStorageType(this.storageTypeId))
                .thenReturn(this.storageType);

        this.mockMvc.perform(delete("/storageType/dstorageType/{id}", this.storageTypeId)
                .contentType(MediaType.APPLICATION_JSON)
                .content(this.jsonStorageType)
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString(expectedResult)));

        verify(this.storageTypeService).deleteStorageType(this.storageTypeId);
    }

    @Test
    public void verifyValidEnableStorageType() throws Exception {
        final String expectedResult = "Enabled " + this.storageTypeId;

        when(this.storageTypeService.enableStorageType(this.storageTypeId))
                .thenReturn(this.storageType);

        this.mockMvc.perform(put("/storageType/estorageType/{id}", this.storageTypeId)
                .contentType(MediaType.APPLICATION_JSON)
                .content(this.jsonStorageType)
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString(expectedResult)));

        verify(this.storageTypeService).enableStorageType(this.storageTypeId);
    }

    @Test
    public void verifyValidAddStorageTypeAttribute() throws Exception {
        when(this.storageTypeService.insertStorageTypeAttributeKey(argThat(new AnyCollectiveStorageTypeAttributeKey())))
                .thenReturn(this.storageTypeAttributeKey);

        this.mockMvc.perform(post("/storageType/storageTypeAttribute")
                .contentType(MediaType.APPLICATION_JSON)
                .content(this.jsonStak)
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.storageDsAttributeKeyName").exists())
                .andExpect(jsonPath("$.storageDsAttributeKeyName").value("storageDsAttributeKeyName"));

        verify(this.storageTypeService)
                .insertStorageTypeAttributeKey(argThat(new AnyCollectiveStorageTypeAttributeKey()));
    }
}
