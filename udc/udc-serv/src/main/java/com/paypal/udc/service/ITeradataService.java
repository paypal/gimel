package com.paypal.udc.service;

import java.util.List;
import com.paypal.udc.entity.teradatapolicy.TeradataPolicy;
import com.paypal.udc.exception.ValidationError;


public interface ITeradataService {

    List<TeradataPolicy> getAllPolicies();

    TeradataPolicy getPolicyBySystemRuleAndDatabase(final long storageSystemId,
            final String databaseName, final String ruleName);

    TeradataPolicy addPolicy(TeradataPolicy policy) throws ValidationError;

    void updatePolicy(TeradataPolicy policy) throws ValidationError;

    void deactivatePolicy(final long policyId) throws ValidationError;

    List<TeradataPolicy> getPoliciesByDatabaseAndSystem(long storageSystemId, String databaseName);

}
