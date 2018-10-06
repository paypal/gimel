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
import com.paypal.udc.config.UDCInterceptorConfig;
import com.paypal.udc.entity.Cluster;
import com.paypal.udc.interceptor.UDCInterceptor;
import com.paypal.udc.service.IClusterService;


@RunWith(SpringRunner.class)
@WebMvcTest(ClusterController.class)
public class ClusterControllerTest {

    private MockMvc mockMvc;

    @MockBean
    private IClusterService clusterService;

    @MockBean
    private UDCInterceptor udcInterceptor;

    @MockBean
    private UDCInterceptorConfig udcInterceptorConfig;

    @Autowired
    private WebApplicationContext webApplicationContext;

    final Gson gson = new Gson();

    private long clusterId, clusterIdUpd;
    private String clusterName, clusterNameUpd;
    private Cluster cluster, clusterUpd;
    private List<Cluster> clusterList;
    private String jsonCluster;

    class AnyCluster extends ArgumentMatcher<Cluster> {
        @Override
        public boolean matches(final Object object) {
            return object instanceof Cluster;
        }
    }

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        this.mockMvc = MockMvcBuilders.webAppContextSetup(this.webApplicationContext).build();

        this.clusterId = 1L;
        this.clusterIdUpd = 2L;
        this.clusterName = "Cluster1";
        this.clusterNameUpd = "Cluster2";
        this.cluster = new Cluster(this.clusterId, this.clusterName, "Description1", "a.b.c.d", 8989, "Y", "CrUser",
                "CrTime",
                "UpdUser", "UpdTime");
        this.clusterUpd = new Cluster(this.clusterIdUpd, this.clusterNameUpd, "Description2", "a.b.c.d", 8989, "Y",
                "CrUser", "CrTime", "UpdUser", "UpdTime");
        this.clusterList = Arrays.asList(this.cluster, this.clusterUpd);

        this.jsonCluster = "{" +
                "\"clusterDescription\": \"Description1\", " +
                "\"clusterId\": 1, " +
                "\"clusterName\": \"Cluster1\", " +
                "\"createdTimestamp\": \"CrTime\", " +
                "\"createdUser\": \"CrUser\", " +
                "\"isActiveYN\": \"Y\", " +
                "\"updatedUser\": \"UpdUser\"" +
                "}";
    }

    @Test
    public void verifyValidGetClusterById() throws Exception {
        when(this.clusterService.getClusterById(this.clusterId))
                .thenReturn(this.cluster);

        this.mockMvc.perform(get("/cluster/cluster/{id}", this.clusterId)
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.clusterId").value(this.clusterId));

        verify(this.clusterService).getClusterById(this.clusterId);
    }

    @Test
    public void verifyValidGetClusterByName() throws Exception {
        when(this.clusterService.getClusterByName(this.clusterName))
                .thenReturn(this.cluster);

        this.mockMvc.perform(get("/cluster/clusterByName/{name:.+}", this.clusterName)
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.clusterName").value(this.clusterName));

        verify(this.clusterService).getClusterByName(this.clusterName);
    }

    @Test
    public void verifyValidGetAllClusters() throws Exception {
        when(this.clusterService.getAllClusters())
                .thenReturn(this.clusterList);

        this.mockMvc.perform(get("/cluster/clusters")
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(this.clusterList.size())));

        verify(this.clusterService).getAllClusters();
    }

    @Test
    public void verifyValidAddStorage() throws Exception {
        when(this.clusterService.addCluster(argThat(new AnyCluster())))
                .thenReturn(this.cluster);

        this.mockMvc.perform(post("/cluster/cluster")
                .contentType(MediaType.APPLICATION_JSON)
                .content(this.jsonCluster)
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.clusterId").exists())
                .andExpect(jsonPath("$.clusterId").value(this.clusterId));

        verify(this.clusterService).addCluster(argThat(new AnyCluster()));
    }

    @Test
    public void verifyValidUpdateStorage() throws Exception {
        when(this.clusterService.updateCluster(argThat(new AnyCluster())))
                .thenReturn(this.cluster);

        this.mockMvc.perform(put("/cluster/cluster")
                .contentType(MediaType.APPLICATION_JSON)
                .content(this.jsonCluster)
                .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.clusterId").exists())
                .andExpect(jsonPath("$.clusterId").value(this.clusterId));

        verify(this.clusterService).updateCluster(argThat(new AnyCluster()));
    }

    @Test
    public void verifyValidDeActivateCluster() throws Exception {
        final String expectedResult = "Deactivated " + this.clusterId;

        when(this.clusterService.deActivateCluster(this.clusterId))
                .thenReturn(this.cluster);

        this.mockMvc.perform(delete("/cluster/dcluster/{id}", this.clusterId)
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString(expectedResult)));

        verify(this.clusterService).deActivateCluster(this.clusterId);
    }

    @Test
    public void verifyValidReActivateCluster() throws Exception {
        final String expectedResult = "Reactivated " + this.clusterId;

        when(this.clusterService.reActivateCluster(this.clusterId))
                .thenReturn(this.cluster);

        this.mockMvc.perform(put("/cluster/ecluster/{id}", this.clusterId)
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString(expectedResult)));

        verify(this.clusterService).reActivateCluster(this.clusterId);
    }
}
