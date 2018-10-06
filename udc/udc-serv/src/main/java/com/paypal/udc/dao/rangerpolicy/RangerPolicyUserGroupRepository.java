package com.paypal.udc.dao.rangerpolicy;

import java.util.List;
import org.springframework.data.repository.CrudRepository;
import com.paypal.udc.entity.rangerpolicy.DerivedPolicyItem;


public interface RangerPolicyUserGroupRepository extends CrudRepository<DerivedPolicyItem, Long> {

    List<DerivedPolicyItem> findByDerivedPolicyId(final long policyId);

}
