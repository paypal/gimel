package com.paypal.udc.service;

import java.util.List;
import com.paypal.udc.entity.rangerpolicy.DerivedPolicy;
import com.paypal.udc.exception.ValidationError;


public interface IRangerService {

    List<DerivedPolicy> getAllPolicies(long clusterId);

    List<DerivedPolicy> getPolicyByPolicyLocations(String locations, String type, long clusterId);

    DerivedPolicy addPolicy(DerivedPolicy policy) throws ValidationError;

    void updatePolicy(DerivedPolicy policy) throws ValidationError;

    DerivedPolicy getPolicyByClusterIdAndPolicyId(final long clusterId, final int policyId);

    void deactivatePolicy(final long derivedPolicyId) throws ValidationError;

}
