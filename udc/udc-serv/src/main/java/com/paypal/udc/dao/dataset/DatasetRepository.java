package com.paypal.udc.dao.dataset;

import java.util.List;
import java.util.Set;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import com.paypal.udc.entity.dataset.Dataset;


public interface DatasetRepository extends CrudRepository<Dataset, Long> {

    public List<Dataset> findByStorageDataSetNameAndIsActiveYN(final String storageDataSetName,
            final String isActiveYN);

    @Query(value = "SELECT storage_dataset_id,concat(\"[\",GROUP_CONCAT(distinct(concat('{\"storageDsFieldName\":\"',storage_ds_field_name,"
            + "'\",\"storageDsFieldDatatype\":\"',storage_ds_field_datatype,'\",\"storageDsFieldDesc\":\"',storage_ds_field_desc,"
            + "'\",\"storageDsFieldId\":\"',storage_ds_field_id,'\",\"storageDataSetId\":\"',storage_dataset_id,"
            + "'\",\"storageDsFieldIsPartition\":\"',storage_ds_field_is_partition,'\"}'))),\"]\") as dataset_schema "
            + "FROM udc.pc_storage_ds_field group by storage_dataset_id", nativeQuery = true)
    public List<Object[]> findDatasetSchema();

    @Query(value = "select a.storage_dataset_id as storage_dataset_id,concat(\"[\",GROUP_CONCAT(distinct(concat('{\"storageDsAttributeKeyName\":\"',"
            + "b.storage_ds_attribute_key_name,'\",\"storageDsAttributeValue\":\"',a.storage_ds_attribute_value,'\"}'))),\"]\") as dataset_properties "
            + "from udc.pc_storage_dataset_attribute_value a join udc.pc_storage_type_attribute_key b on "
            + "b.storage_ds_attribute_key_id=a.storage_ds_attribute_key_id where a.storage_dataset_id=:dataSetId group "
            + "by a.storage_dataset_id", nativeQuery = true)
    public List<Object[]> findDatasetProperties(@Param("dataSetId") Long dataSetId);

    @Query(value = "select a.storage_system_id as storage_system_id,concat(\"[\",GROUP_CONCAT(distinct(concat('{\"storageDsAttributeKeyName\":\"',"
            + "b.storage_ds_attribute_key_name,'\",\"storageSystemAttributeValue\":\"',a.storage_system_attribute_value,'\"}'))),\"]\")  as system_properties "
            + "from udc.pc_storage_system_attribute_value a join udc.pc_storage_type_attribute_key b on a.storage_ds_attribute_key_id=b.storage_ds_attribute_key_id "
            + "where a.storage_system_id=:storageSystemId group by a.storage_system_id", nativeQuery = true)
    public List<Object[]> findSystemProperties(@Param("storageSystemId") Long storageSystemId);

    public List<Dataset> findFirst25ByIsActiveYNAndStorageDataSetNameContainingAndStorageDataSetIdIn(
            final String isActiveYN,
            final String dataSetSubString, final Set<Long> datasetIds);

    public List<Dataset> findByStorageDataSetIdIn(final List<Long> datasetIds);

    public List<Dataset> findByIsActiveYN(String isActiveYN);

    public List<Dataset> findByObjectSchemaMapId(final long objectId);

    public List<Dataset> findByIsActiveYNAndStorageDataSetNameContaining(final String isActiveYN,
            final String datasetStr);

    @Query(value = "SELECT type_id, type_name, "
            + "CONCAT(\"[\",GROUP_CONCAT("
            + "CONCAT('{',"
            + "'\"storageDataSetId\":',storage_dataset_id,',"
            + "\"storageDataSetName\":\"', storage_dataset_name, "
            + "'\", \"storageDataSetAliasName\":\"',storage_dataset_alias_name,"
            + "'\", \"storageDatabaseName\":\"',storage_database_name,"
            + "'\", \"storageDataSetDescription\":\"',storage_dataset_desc,"
            + "'\", \"objectSchemaMapId\":',object_schema_map_id,',"
            + "\"storageSystemId\":',storage_system_id,',"
            + "\"userId\":',user_id,', "
            + "\"isAutoRegistered\":\"',is_auto_registered,"
            + "'\"}')),\"]\") "
            + "FROM (SELECT *,@type_rank \\:= IF(@current_type = type_id, @type_rank + 1, 1) "
            + "AS type_rank, @current_type \\:= type_id FROM (SELECT g.storage_type_id as type_id, g.storage_type_name as "
            + "type_name,d.* from udc.pc_storage_dataset d, udc.pc_object_schema_map e , "
            + "udc.pc_storage_system f, udc.pc_storage_type g where d.storage_dataset_name "
            + "like %:datasetSubString% and d.is_active_y_n=:isActiveYN and d.object_schema_map_id=e.object_id and "
            + "f.storage_system_id=e.storage_system_id and g.storage_type_id = f.storage_type_id) dataset_type) "
            + "ranked WHERE type_rank <= 20 group by type_id, type_name", nativeQuery = true)
    public List<Object[]> getAllDatasetsWithType(@Param("isActiveYN") String isActiveYN,
            @Param("datasetSubString") String datasetSubString);

    @Query(value = "select d.storage_dataset_id,d.storage_dataset_name,d.object_schema_map_id,d.storage_dataset_alias_name, "
            + "d.is_auto_registered,d.cre_user,d.cre_ts,d.upd_user,d.upd_ts,d.is_active_y_n,e.storage_system_id,"
            + "e.query,e.object_schema,f.storage_system_name from udc.pc_storage_dataset d, "
            + "udc.pc_object_schema_map e, udc.pc_storage_system f where storage_dataset_id=:dataSetId "
            + "and d.object_schema_map_id=e.object_id and e.storage_system_id=f.storage_system_id", nativeQuery = true)
    public List<Object[]> getDatasetDetails(@Param("dataSetId") final long dataSetId);

    // + "'\", \"createdUser\":\"',cre_user,"
    // + "'\", \"updatedUser\":\"',upd_user,"
    // + "'\", \"updatedTimestamp\":\"',upd_ts,"
    // + "'\", \"createdTimestamp\":\"',cre_ts,"
    // + "'\", \"isActiveYN\":\"',is_active_y_n,"
}
