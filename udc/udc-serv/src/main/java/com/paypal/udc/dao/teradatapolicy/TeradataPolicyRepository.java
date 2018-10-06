package com.paypal.udc.dao.teradatapolicy;

import java.util.List;
import org.springframework.data.repository.CrudRepository;
import com.paypal.udc.entity.teradatapolicy.TeradataPolicy;


public interface TeradataPolicyRepository extends CrudRepository<TeradataPolicy, Long> {

    TeradataPolicy findByStorageSystemIdAndDatabaseNameAndIamRoleName(long storageSystemId,
            String databaseName, String roleName);

    List<TeradataPolicy> findByStorageSystemIdAndDatabaseName(long storageSystemId, String databaseName);

}
