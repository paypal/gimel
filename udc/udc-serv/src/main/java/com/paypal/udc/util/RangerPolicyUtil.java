package com.paypal.udc.util;

import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import com.paypal.udc.dao.rangerpolicy.RangerPolicyRepository;
import com.paypal.udc.dao.rangerpolicy.RangerPolicyUserGroupRepository;
import com.paypal.udc.entity.rangerpolicy.DerivedPolicy;
import com.paypal.udc.entity.rangerpolicy.DerivedPolicyItem;


@Component
public class RangerPolicyUtil {

    final static Logger logger = LoggerFactory.getLogger(RangerPolicyUtil.class);
    @Autowired
    private RangerPolicyRepository rangerPolicyRepository;
    @Autowired
    private RangerPolicyUserGroupRepository rangerPolicyUserGroupRepository;

    public List<DerivedPolicy> computePoliciesByLocation(final String location, final String type,
            final long clusterId) {
        List<DerivedPolicy> derivedPolicies = new ArrayList<DerivedPolicy>();
        String tempLoc = location;
        int i = 0;
        while (derivedPolicies.size() == 0 && i < 5) {
            derivedPolicies = this.rangerPolicyRepository
                    .findByPolicyLocationsContainingAndTypeNameAndClusterId(tempLoc, type, clusterId);
            if (tempLoc.contains("/")) {
                tempLoc = tempLoc.substring(0, tempLoc.lastIndexOf("/"));
            }
            if (derivedPolicies != null && derivedPolicies.size() > 0) {
                derivedPolicies.forEach(policy -> {
                    final long derivedPolicyId = policy.getDerivedPolicyId();
                    final List<DerivedPolicyItem> policyItems = this.rangerPolicyUserGroupRepository
                            .findByDerivedPolicyId(derivedPolicyId);
                    policy.setPolicyItems(policyItems);
                });
                break;
            }
            i++;
        }
        return derivedPolicies;
    }
}
