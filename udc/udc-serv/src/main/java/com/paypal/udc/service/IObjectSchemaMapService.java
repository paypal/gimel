package com.paypal.udc.service;

import java.util.List;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import com.paypal.udc.entity.dataset.Dataset;
import com.paypal.udc.entity.objectschema.CollectiveObjectSchemaMap;
import com.paypal.udc.entity.objectschema.ObjectSchemaMap;
import com.paypal.udc.exception.ValidationError;


public interface IObjectSchemaMapService {

    Page<CollectiveObjectSchemaMap> getPagedObjectMappings(final long storageSystemId, final Pageable pageable);

    ObjectSchemaMap getDatasetById(long topicId);

    ObjectSchemaMap addObjectSchema(ObjectSchemaMap topic) throws ValidationError;

    List<String> getDistinctContainerNamesByStorageSystemId(final long storageSystemId);

    List<String> getDistinctObjectNames(final String containerName, final long storageSystemId);

    // List<CollectiveObjectSchemaMap> getUnRegisteredObjects(final long systemId);

    Page<CollectiveObjectSchemaMap> getPagedUnRegisteredObjects(final long systemId, final Pageable pageable);

    ObjectSchemaMap updateObjectSchemaMap(final ObjectSchemaMap schemaMap) throws ValidationError;

    List<Dataset> getDatasetBySystemContainerAndObject(String systemName, String containerName, String objectName)
            throws ValidationError;

    List<ObjectSchemaMap> getObjectSchemaMapsBySystemIds(final long storageSystemId);

    List<CollectiveObjectSchemaMap> getSchemaBySystemContainerAndObject(final long systemId, String containerName,
            String objectName);

    void deActivateObjectAndDataset(final long objectId) throws ValidationError;

    List<String> getDistinctContainerNames();

    Page<ObjectSchemaMap> getObjectsByStorageSystemAndContainer(String storageSystemName, String containerName,
            final Pageable pageable);

}
