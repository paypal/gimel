package com.paypal.udc.dao.rangerpolicy;

import java.util.List;
import org.springframework.data.repository.CrudRepository;
import com.paypal.udc.entity.rangerpolicy.DerivedPolicy;


public interface RangerPolicyRepository extends CrudRepository<DerivedPolicy, Long> {

    List<DerivedPolicy> findByPolicyLocationsContainingAndTypeNameAndClusterId(String location, String typeName,
            long clusterId);

    DerivedPolicy findByClusterIdAndPolicyId(long clusterId, int policyId);

    List<DerivedPolicy> findByClusterIdAndIsActiveYN(long clusterId, String isActiveYN);

}
