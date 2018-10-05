package com.paypal.udc.dao.objectschema;

import java.util.List;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.repository.PagingAndSortingRepository;
import com.paypal.udc.entity.objectschema.ObjectSchemaMap;


public interface PageableObjectSchemaMapRepository extends PagingAndSortingRepository<ObjectSchemaMap, Long> {

    @Override
    public Page<ObjectSchemaMap> findAll(Pageable pageable);

    public Page<ObjectSchemaMap> findByStorageSystemIdIn(final List<Long> storageSystemIds, Pageable pageable);

    public Page<ObjectSchemaMap> findByStorageSystemIdAndContainerName(final long storageSystemId,
            final String containerName, Pageable pageable);

    public Page<ObjectSchemaMap> findByStorageSystemId(final long storageSystemId, Pageable pageable);

    public Page<ObjectSchemaMap> findByStorageSystemIdAndIsSelfDiscovered(final long storageSystemId,
            final String isSelfDiscovered, Pageable pageable);

    public Page<ObjectSchemaMap> findByContainerName(final String containerName, Pageable pageable);

    public Page<ObjectSchemaMap> findByContainerNameAndStorageSystemIdIn(final List<Long> storageSystemIds,
            final String containerName, Pageable pageable);

    public Page<ObjectSchemaMap> findByStorageSystemIdInAndIsActiveYN(final List<Long> storageSystemIds,
            final String isActiveYN, Pageable pageable);

    public Page<ObjectSchemaMap> findByStorageSystemId(final long storageSystemId, final String isActiveYN,
            Pageable pageable);

    public Page<ObjectSchemaMap> findByStorageSystemIdAndIsActiveYNAndIsRegisteredAndIsSelfDiscovered(
            final long storageSystemId, final String isActiveYN, final String isRegistered,
            final String isSelfDiscovered, Pageable pageable);
}
