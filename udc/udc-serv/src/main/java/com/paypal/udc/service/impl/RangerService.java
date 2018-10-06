package com.paypal.udc.service.impl;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import javax.validation.ConstraintViolationException;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import com.google.common.collect.Lists;
import com.paypal.udc.dao.rangerpolicy.RangerPolicyRepository;
import com.paypal.udc.dao.rangerpolicy.RangerPolicyUserGroupRepository;
import com.paypal.udc.entity.rangerpolicy.DerivedPolicy;
import com.paypal.udc.entity.rangerpolicy.DerivedPolicyItem;
import com.paypal.udc.exception.ValidationError;
import com.paypal.udc.service.IRangerService;
import com.paypal.udc.util.ClusterUtil;
import com.paypal.udc.util.enumeration.ActiveEnumeration;


@Service
@Transactional(propagation = Propagation.SUPPORTS, readOnly = true)
public class RangerService implements IRangerService {

    final static Logger logger = Logger.getLogger(RangerService.class);
    @Autowired
    private RangerPolicyRepository rangerPolicyRepository;
    @Autowired
    private RangerPolicyUserGroupRepository rangerPolicyUserGroupRepository;
    @Autowired
    private ClusterUtil clusterUtil;

    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss");
    @Value("${application.defaultuser}")
    private String user;

    @Override
    public List<DerivedPolicy> getAllPolicies(final long clusterId) {

        final List<DerivedPolicy> derivedPolicies = new ArrayList<DerivedPolicy>();
        this.rangerPolicyRepository.findByClusterIdAndIsActiveYN(clusterId, ActiveEnumeration.YES.getFlag())
                .forEach(policy -> {
                    final long derivedPolicyId = policy.getDerivedPolicyId();
                    final List<DerivedPolicyItem> policyItems = this.rangerPolicyUserGroupRepository
                            .findByDerivedPolicyId(derivedPolicyId);
                    policy.setPolicyItems(policyItems);
                    derivedPolicies.add(policy);
                });
        return derivedPolicies;
    }

    @Override
    public List<DerivedPolicy> getPolicyByPolicyLocations(final String location, final String type,
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

    @Override
    @Transactional(propagation = Propagation.REQUIRED, readOnly = false, rollbackFor = {
            TransactionSystemException.class, DataIntegrityViolationException.class, IndexOutOfBoundsException.class,
            ValidationError.class })
    public void updatePolicy(final DerivedPolicy policy) throws ValidationError {
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final String time = sdf.format(timestamp);
        DerivedPolicy tempPolicy = new DerivedPolicy();
        final ValidationError v = new ValidationError();
        if (policy.getDerivedPolicyId() == 0) {
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("Not valid for Update.");
            throw v;
        }
        try {
            this.clusterUtil.validateCluster(policy.getClusterId());
            tempPolicy = new DerivedPolicy(policy.getDerivedPolicyId(), policy.getPolicyId(), policy.getClusterId(),
                    policy.getPolicyName(), policy.getTypeName(), policy.getPolicyLocations(),
                    ActiveEnumeration.YES.getFlag(), this.user, time, this.user, time);
            tempPolicy = this.rangerPolicyRepository.save(tempPolicy);
        }
        catch (final DataIntegrityViolationException e) {
            v.setErrorCode(HttpStatus.CONFLICT);
            v.setErrorDescription("Cluster Id and policy ID are duplicated");
            throw v;
        }
        catch (final ConstraintViolationException e) {
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("Policy name is empty");
            throw v;
        }
        final long derivedPolicyId = tempPolicy.getDerivedPolicyId();
        final List<DerivedPolicyItem> policyItems = policy.getPolicyItems();
        if (policyItems != null && policy.getPolicyItems().size() > 0) {
            policyItems.forEach(policyItem -> {
                if (policyItem.getDerivedPolicyUserGroupID() == 0) {
                    policyItem.setCreatedUser(this.user);
                    policyItem.setUpdatedUser(this.user);
                    policyItem.setCreatedTimestamp(time);
                    policyItem.setUpdatedTimestamp(time);
                    policyItem.setDerivedPolicyId(derivedPolicyId);
                    policyItem.setIsActiveYN(ActiveEnumeration.YES.getFlag());
                    this.rangerPolicyUserGroupRepository.save(policyItem);
                }
                else {
                    final DerivedPolicyItem dpi = new DerivedPolicyItem(policyItem.getDerivedPolicyUserGroupID(),
                            policyItem.getDerivedPolicyId(), policyItem.getAccessTypes(), policyItem.getUsers(),
                            policyItem.getGroups(), ActiveEnumeration.YES.getFlag(), this.user, time, this.user, time);
                    this.rangerPolicyUserGroupRepository.save(dpi);
                }
            });
        }
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED, readOnly = false, rollbackFor = { ValidationError.class,
            ConstraintViolationException.class, DataIntegrityViolationException.class })
    public DerivedPolicy addPolicy(final DerivedPolicy policy) throws ValidationError {
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final String time = sdf.format(timestamp);
        DerivedPolicy tempPolicy = new DerivedPolicy();
        final ValidationError v = new ValidationError();
        // insert into pc_ranger_policy table
        try {
            this.clusterUtil.validateCluster(policy.getClusterId());
            policy.setIsActiveYN(ActiveEnumeration.YES.getFlag());
            tempPolicy = new DerivedPolicy(policy.getPolicyId(), policy.getClusterId(), policy.getPolicyName(),
                    policy.getTypeName(), policy.getPolicyLocations(),
                    ActiveEnumeration.YES.getFlag(), this.user, time, this.user, time);
            tempPolicy = this.rangerPolicyRepository.save(tempPolicy);
        }
        catch (final DataIntegrityViolationException e) {
            v.setErrorCode(HttpStatus.CONFLICT);
            v.setErrorDescription("Cluster Id and policy ID are duplicated");
            throw v;
        }
        catch (final ConstraintViolationException e) {
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("Policy name is empty");
            throw v;
        }

        // insert into pc_ranger_policy_user_group
        final long derivedPolicyId = tempPolicy.getDerivedPolicyId();
        final List<DerivedPolicyItem> policyItems = policy.getPolicyItems();
        if (policyItems != null && policy.getPolicyItems().size() > 0) {
            policyItems.forEach(policyItem -> {
                policyItem.setCreatedUser(this.user);
                policyItem.setUpdatedUser(this.user);
                policyItem.setCreatedTimestamp(time);
                policyItem.setUpdatedTimestamp(time);
                policyItem.setDerivedPolicyId(derivedPolicyId);
                policyItem.setIsActiveYN(ActiveEnumeration.YES.getFlag());
            });

            final List<DerivedPolicyItem> tempPolicyItems = Lists
                    .newArrayList(this.rangerPolicyUserGroupRepository.save(policyItems));
            tempPolicy.setPolicyItems(tempPolicyItems);
        }
        return tempPolicy;

    }

    @Override
    public DerivedPolicy getPolicyByClusterIdAndPolicyId(final long clusterId, final int policyId) {
        final DerivedPolicy derivedPolicy = this.rangerPolicyRepository.findByClusterIdAndPolicyId(clusterId, policyId);
        final List<DerivedPolicyItem> policyItems = this.rangerPolicyUserGroupRepository
                .findByDerivedPolicyId(derivedPolicy.getDerivedPolicyId());
        if (policyItems == null || policyItems.size() == 0) {
            derivedPolicy.setPolicyItems(new ArrayList<DerivedPolicyItem>());
        }
        else {
            derivedPolicy.setPolicyItems(policyItems);
        }
        return derivedPolicy;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED, readOnly = false, rollbackFor = { ValidationError.class,
            ConstraintViolationException.class, DataIntegrityViolationException.class })
    public void deactivatePolicy(final long derivedPolicyId) throws ValidationError {
        final ValidationError v = new ValidationError();
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final String time = sdf.format(timestamp);
        final DerivedPolicy derivedPolicy = this.rangerPolicyRepository.findOne(derivedPolicyId);
        if (derivedPolicy == null) {
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("Derived Policy ID not found");
            throw v;
        }
        derivedPolicy.setUpdatedTimestamp(time);
        derivedPolicy.setUpdatedUser(this.user);
        derivedPolicy.setIsActiveYN(ActiveEnumeration.NO.getFlag());
        this.rangerPolicyRepository.save(derivedPolicy);

        final List<DerivedPolicyItem> policyItems = this.rangerPolicyUserGroupRepository
                .findByDerivedPolicyId(derivedPolicyId);
        if (policyItems != null && policyItems.size() > 0) {
            policyItems.forEach(policyItem -> {
                policyItem.setUpdatedTimestamp(time);
                policyItem.setUpdatedUser(this.user);
                policyItem.setIsActiveYN(ActiveEnumeration.NO.getFlag());
            });
            this.rangerPolicyUserGroupRepository.save(policyItems);
        }
    }

}
