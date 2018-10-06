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
import com.paypal.udc.cache.StorageCache;
import com.paypal.udc.config.UDCInterceptorConfig;
import com.paypal.udc.dao.StorageRepository;
import com.paypal.udc.entity.Storage;
import com.paypal.udc.interceptor.UDCInterceptor;
import com.paypal.udc.service.IStorageService;


@RunWith(SpringRunner.class)
@WebMvcTest(StorageController.class)
public class StorageControllerTest {

    private MockMvc mockMvc;

    @MockBean
    private IStorageService storageService;

    @MockBean
    private StorageRepository storageRepository;

    @MockBean
    private StorageCache storageCache;

    @MockBean
    private UDCInterceptor udcInterceptor;

    @MockBean
    private UDCInterceptorConfig udcInterceptorConfig;

    @Autowired
    private WebApplicationContext webApplicationContext;

    final Gson gson = new Gson();

    private Long storageId;
    private String storageName, storageDescription, createdUser, createdTimestamp, updatedUser, updatedTimestamp;
    private Storage storage;
    private String jsonStorage;
    private List<Storage> storageList;

    class AnyStorage extends ArgumentMatcher<Storage> {
        @Override
        public boolean matches(final Object object) {
            return object instanceof Storage;
        }
    }

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        this.mockMvc = MockMvcBuilders.webAppContextSetup(this.webApplicationContext).build();

        this.storageId = 1L;
        this.storageName = "StorageName";
        this.storageDescription = "Storage Description";
        this.createdUser = "crUser";
        this.createdTimestamp = "crTime";
        this.updatedUser = "updUser";
        this.updatedTimestamp = "updTime";
        this.storage = new Storage(this.storageId, this.storageName, this.storageDescription, this.createdUser,
                this.createdTimestamp, this.updatedUser, this.updatedTimestamp);
        this.storageList = Arrays.asList(this.storage);
        this.jsonStorage = "{" +
                "\"createdTimestamp\": \"string\", " +
                "\"createdUser\": \"string\", " +
                "\"isActiveYN\": \"string\", " +
                "\"storageDescription\": \"string\", " +
                "\"storageId\": 1, " +
                "\"storageName\": \"string\", " +
                "\"updatedUser\": \"string\"" +
                "}";
    }

    @Test
    public void verifyValidGetStorageById() throws Exception {
        when(this.storageCache.getStorage(this.storageId))
                .thenReturn(this.storage);

        this.mockMvc.perform(get("/storage/storage/{id}", this.storageId)
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.storageName").value(this.storageName));

        verify(this.storageCache).getStorage(this.storageId);
    }

    @Test
    public void verifyValidGetStorageByTitle() throws Exception {
        when(this.storageService.getStorageByName(this.storageName))
                .thenReturn(this.storage);

        this.mockMvc.perform(get("/storage/storageByName/{name:.+}", this.storageName)
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.storageName").value(this.storageName));

        verify(this.storageService).getStorageByName(this.storageName);
    }

    @Test
    public void verifyValidGetAllStorages() throws Exception {
        when(this.storageService.getAllStorages())
                .thenReturn(this.storageList);

        this.mockMvc.perform(get("/storage/storages")
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(this.storageList.size())));

        verify(this.storageService).getAllStorages();
    }

    @Test
    public void verifyValidAddStorage() throws Exception {
        when(this.storageService.addStorage(argThat(new AnyStorage())))
                .thenReturn(this.storage);

        this.mockMvc.perform(post("/storage/storage")
                .contentType(MediaType.APPLICATION_JSON)
                .content(this.jsonStorage)
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.storageName").exists())
                .andExpect(jsonPath("$.storageName").value(this.storageName));

        verify(this.storageService).addStorage(argThat(new AnyStorage()));
    }

    @Test
    public void verifyValidUpdateStorage() throws Exception {
        when(this.storageService.updateStorage(argThat(new AnyStorage())))
                .thenReturn(this.storage);

        this.mockMvc.perform(put("/storage/storage")
                .contentType(MediaType.APPLICATION_JSON)
                .content(this.jsonStorage)
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.storageName").exists())
                .andExpect(jsonPath("$.storageName").value(this.storageName));

        verify(this.storageService).updateStorage(argThat(new AnyStorage()));
    }

    @Test
    public void verifyValidDeleteStorage() throws Exception {
        final String expectedResult = "Deactivated " + this.storageId;

        when(this.storageService.deleteStorage(this.storageId))
                .thenReturn(this.storage);

        this.mockMvc.perform(delete("/storage/dstorage/{id}", this.storageId)
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString(expectedResult)));

        verify(this.storageService).deleteStorage(this.storageId);
    }

    @Test
    public void verifyValidEnableStorage() throws Exception {
        final String expectedResult = "Enabled " + this.storageId;

        when(this.storageService.enableStorage(this.storageId))
                .thenReturn(this.storage);

        this.mockMvc.perform(put("/storage/estorage/{id}", this.storageId)
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString(expectedResult)));

        verify(this.storageService).enableStorage(this.storageId);
    }
}
