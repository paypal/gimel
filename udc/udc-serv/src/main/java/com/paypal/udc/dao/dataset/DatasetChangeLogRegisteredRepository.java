package com.paypal.udc.dao.dataset;

import java.util.List;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import com.paypal.udc.entity.dataset.DatasetChangeLogRegistered;


public interface DatasetChangeLogRegisteredRepository extends CrudRepository<DatasetChangeLogRegistered, Long> {

    public DatasetChangeLogRegistered findByStorageDataSetName(String storageDataSetName);

    @Query(value = "  select r1.* from udc.pc_storage_dataset_change_log_registered r1 inner join "
            + "(select max(storage_dataset_change_log_id) as change_log_id, storage_dataset_id "
            + "from udc.pc_storage_dataset_change_log_registered WHERE storage_cluster_id = :clusterId and "
            + "storage_deployment_status is null group by storage_dataset_id) r2  on r2.storage_dataset_id=r1.storage_dataset_id "
            + "and r1.storage_dataset_change_log_id=r2.change_log_id where r1.storage_cluster_id =:clusterId", nativeQuery = true)
    public List<DatasetChangeLogRegistered> findChanges(@Param("clusterId") long clusterId);

    public List<DatasetChangeLogRegistered> findByStorageDataSetId(final long storageDataSetId);

    public List<DatasetChangeLogRegistered> findByStorageClusterId(long storageClusterId);

}
