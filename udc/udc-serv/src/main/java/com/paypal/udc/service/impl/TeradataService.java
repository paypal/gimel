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
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import com.paypal.udc.dao.teradatapolicy.TeradataPolicyRepository;
import com.paypal.udc.entity.teradatapolicy.TeradataPolicy;
import com.paypal.udc.exception.ValidationError;
import com.paypal.udc.service.ITeradataService;
import com.paypal.udc.util.StorageSystemUtil;
import com.paypal.udc.util.enumeration.ActiveEnumeration;


@Service
@Transactional(propagation = Propagation.SUPPORTS, readOnly = true)
public class TeradataService implements ITeradataService {

    final static Logger logger = Logger.getLogger(TeradataService.class);
    @Autowired
    private TeradataPolicyRepository teradataPolicyRepository;
    @Autowired
    private StorageSystemUtil storageSystemUtil;

    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss");
    @Value("${application.defaultuser}")
    private String user;

    @Override
    public List<TeradataPolicy> getAllPolicies() {

        final List<TeradataPolicy> derivedPolicies = new ArrayList<TeradataPolicy>();
        this.teradataPolicyRepository.findAll()
                .forEach(policy -> {
                    derivedPolicies.add(policy);
                });
        return derivedPolicies;
    }

    @Override
    public TeradataPolicy getPolicyBySystemRuleAndDatabase(final long storageSystemId,
            final String databaseName, final String ruleName) {

        return this.teradataPolicyRepository.findByStorageSystemIdAndDatabaseNameAndIamRoleName(
                storageSystemId, databaseName, ruleName);
    }

    @Override
    public void updatePolicy(final TeradataPolicy policy) throws ValidationError {
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final String time = sdf.format(timestamp);
        TeradataPolicy tempPolicy = new TeradataPolicy();
        final ValidationError v = new ValidationError();
        if (policy.getTeradataPolicyId() == 0) {
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("Not valid for Update.");
            throw v;
        }
        try {
            this.storageSystemUtil.validateStorageSystem(policy.getStorageSystemId());
            tempPolicy = new TeradataPolicy(policy.getTeradataPolicyId(), policy.getStorageSystemId(),
                    policy.getDatabaseName(), policy.getIamRoleName(), this.user, time, this.user,
                    time, ActiveEnumeration.YES.getFlag());
            tempPolicy = this.teradataPolicyRepository.save(tempPolicy);
        }
        catch (final DataIntegrityViolationException e) {
            v.setErrorCode(HttpStatus.CONFLICT);
            v.setErrorDescription("Storage System, Mapped Role, Database name and Role Name are duplicated");
            throw v;
        }
        catch (final ConstraintViolationException e) {
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("Storage System or Mapped Role or Database name or Role Name are");
            throw v;
        }
    }

    @Override
    public TeradataPolicy addPolicy(final TeradataPolicy policy) throws ValidationError {
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final String time = sdf.format(timestamp);
        TeradataPolicy tempPolicy = new TeradataPolicy();
        final ValidationError v = new ValidationError();
        // insert into pc_ranger_policy table
        try {
            this.storageSystemUtil.validateStorageSystem(policy.getStorageSystemId());
            tempPolicy = new TeradataPolicy(policy.getStorageSystemId(),
                    policy.getDatabaseName(), policy.getIamRoleName(), this.user, time, this.user,
                    time, ActiveEnumeration.YES.getFlag());
            tempPolicy = this.teradataPolicyRepository.save(tempPolicy);
        }
        catch (final DataIntegrityViolationException e) {
            v.setErrorCode(HttpStatus.CONFLICT);
            v.setErrorDescription("System ID, policy name and database name  are duplicated");
            throw v;
        }
        // catch (final ConstraintViolationException e) {
        // v.setErrorCode(HttpStatus.BAD_REQUEST);
        // v.setErrorDescription("Policy name is empty");
        // throw v;
        // }
        return tempPolicy;
    }

    @Override
    public void deactivatePolicy(final long derivedPolicyId) throws ValidationError {
        final ValidationError v = new ValidationError();
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final String time = sdf.format(timestamp);
        final TeradataPolicy derivedPolicy = this.teradataPolicyRepository.findOne(derivedPolicyId);
        if (derivedPolicy == null) {
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("Teradata Policy ID not found");
            throw v;
        }
        derivedPolicy.setUpdatedTimestamp(time);
        derivedPolicy.setUpdatedUser(this.user);
        derivedPolicy.setIsActiveYN(ActiveEnumeration.NO.getFlag());
        this.teradataPolicyRepository.save(derivedPolicy);

    }

    @Override
    public List<TeradataPolicy> getPoliciesByDatabaseAndSystem(final long storageSystemId, final String databaseName) {
        return this.teradataPolicyRepository.findByStorageSystemIdAndDatabaseName(storageSystemId, databaseName);
    }

}
