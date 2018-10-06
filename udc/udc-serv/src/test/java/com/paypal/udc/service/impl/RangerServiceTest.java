package com.paypal.udc.service.impl;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.Arrays;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;
import com.paypal.udc.config.UDCInterceptorConfig;
import com.paypal.udc.dao.rangerpolicy.RangerPolicyRepository;
import com.paypal.udc.dao.rangerpolicy.RangerPolicyUserGroupRepository;
import com.paypal.udc.entity.rangerpolicy.DerivedPolicy;
import com.paypal.udc.interceptor.UDCInterceptor;
import com.paypal.udc.util.ClusterUtil;
import com.paypal.udc.util.RangerPolicyUtil;
import com.paypal.udc.util.enumeration.ActiveEnumeration;


@RunWith(SpringRunner.class)
public class RangerServiceTest {

    @MockBean
    private UDCInterceptor udcInterceptor;

    @MockBean
    private UDCInterceptorConfig udcInterceptorConfig;

    @Mock
    private RangerPolicyRepository rangerPolicyRepository;

    @Mock
    private RangerPolicyUserGroupRepository rangerPolicyUserGroupRepository;

    @Mock
    private ClusterUtil clusterUtil;

    @Mock
    private RangerPolicyUtil rangerPolicyUtil;

    @InjectMocks
    private RangerService rangerService;

    private Long clusterId;
    private int policyId;
    private Long derivedPolicyId;
    private DerivedPolicy policy;
    private List<DerivedPolicy> policies;
    private String location;
    private String type;

    class AnyPolicy extends ArgumentMatcher<DerivedPolicy> {
        @Override
        public boolean matches(final Object object) {
            return object instanceof DerivedPolicy;
        }
    }

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        this.clusterId = 0L;
        this.derivedPolicyId = 1L;
        this.policyId = 2;
        this.policy = new DerivedPolicy();
        this.policy.setClusterId(this.clusterId);
        this.policy.setDerivedPolicyId(this.derivedPolicyId);
        this.policies = Arrays.asList(this.policy);
        this.location = "location";
        this.type = "hive";
    }

    @Test
    public void verifyValidGetAllPolicies() throws Exception {
        when(this.rangerPolicyRepository.findByClusterIdAndIsActiveYN(this.clusterId, ActiveEnumeration.YES.getFlag()))
                .thenReturn(this.policies);

        final List<DerivedPolicy> result = this.rangerService.getAllPolicies(this.clusterId);
        assertEquals(this.policies, result);

        verify(this.rangerPolicyRepository).findByClusterIdAndIsActiveYN(this.clusterId,
                ActiveEnumeration.YES.getFlag());
    }

    @Test
    public void verifyValidGetPolicyByPolicyLocations() throws Exception {
        when(this.rangerPolicyRepository.findByPolicyLocationsContainingAndTypeNameAndClusterId(this.location,
                this.type, this.clusterId))
                        .thenReturn(this.policies);

        final List<DerivedPolicy> result = this.rangerService.getPolicyByPolicyLocations(this.location, this.type,
                this.clusterId);
        assertEquals(this.policies, result);

        verify(this.rangerPolicyRepository).findByPolicyLocationsContainingAndTypeNameAndClusterId(this.location,
                this.type, this.clusterId);
    }

    @Test
    public void verifyValidAddPolicy() throws Exception {
        when(this.rangerPolicyRepository.save(argThat(new AnyPolicy()))).thenReturn(this.policy);

        final DerivedPolicy result = this.rangerService.addPolicy(this.policy);
        assertEquals(this.policy, result);

        verify(this.rangerPolicyRepository).save(argThat(new AnyPolicy()));
    }

    @Test
    public void verifyValidGetPolicyByClusterIdAndPolicyId() throws Exception {
        when(this.rangerPolicyRepository.findByClusterIdAndPolicyId(this.clusterId, this.policyId))
                .thenReturn(this.policy);

        final DerivedPolicy result = this.rangerService.getPolicyByClusterIdAndPolicyId(this.clusterId, this.policyId);
        assertEquals(this.policy, result);

        verify(this.rangerPolicyRepository).findByClusterIdAndPolicyId(this.clusterId, this.policyId);
    }
}
