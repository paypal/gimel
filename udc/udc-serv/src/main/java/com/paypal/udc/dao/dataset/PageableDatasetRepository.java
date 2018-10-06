package com.paypal.udc.dao.dataset;

import java.util.List;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.repository.PagingAndSortingRepository;
import com.paypal.udc.entity.dataset.Dataset;


public interface PageableDatasetRepository extends PagingAndSortingRepository<Dataset, Long> {

    public Page<Dataset> findByStorageDataSetIdInAndStorageDataSetNameContaining(final List<Long> datasetIds,
            String datasetStr, Pageable pageable);

    public Page<Dataset> findByIsActiveYNAndStorageDataSetNameContaining(final String isActiveYN,
            final String datasetStr, Pageable pageable);

    public Page<Dataset> findByIsActiveYNAndStorageDataSetNameContainingAndStorageSystemIdIn(final String isActiveYN,
            final String datasetStr, Pageable pageable, final List<Long> objectIds);

    public Page<Dataset> findByIsActiveYNAndStorageSystemIdIn(final String isActiveYN, final List<Long> objectIds,
            Pageable pageable);

    public Page<Dataset> findByIsActiveYNAndStorageSystemId(final String isActiveYN, final long storageSystemId,
            final Pageable pageable);

    public Page<Dataset> findByIsActiveYNAndStorageDataSetNameContainingAndStorageSystemId(final String isActiveYN,
            final String datasetStr, final long storageSystemId, final Pageable pageable);

}
