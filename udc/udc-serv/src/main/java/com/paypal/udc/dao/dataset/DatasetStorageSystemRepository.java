package com.paypal.udc.dao.dataset;

import java.util.List;
import org.springframework.data.repository.CrudRepository;
import com.paypal.udc.entity.dataset.DatasetStorageSystem;


public interface DatasetStorageSystemRepository extends CrudRepository<DatasetStorageSystem, Long> {

    public DatasetStorageSystem findByStorageDataSetId(long storageDatasetId);

    public List<DatasetStorageSystem> findByIsActiveYNAndStorageDataSetIdIn(final String isActiveYN,
            final List<Long> storageDatasetIds);

    public List<DatasetStorageSystem> findByStorageSystemIdAndIsActiveYN(long storageSystemId, String isActiveYN);

    public List<DatasetStorageSystem> findByStorageSystemIdInAndIsActiveYN(final List<Long> storageSystemIds,
            String isActiveYN);

}
