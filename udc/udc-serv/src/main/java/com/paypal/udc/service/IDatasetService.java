package com.paypal.udc.service;

import java.util.List;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import com.paypal.udc.entity.dataset.CumulativeDataset;
import com.paypal.udc.entity.dataset.Dataset;
import com.paypal.udc.entity.dataset.DatasetChangeLogRegistered;
import com.paypal.udc.entity.dataset.DatasetWithAttributes;
import com.paypal.udc.exception.ValidationError;


public interface IDatasetService {

    List<CumulativeDataset> getAllDatasets(final String dataSetSubString);

    Dataset getDataSetById(long datasetId);

    DatasetWithAttributes getDataSetByName(String dataSetName) throws ValidationError;

    Dataset updateDataSet(Dataset dataSet) throws ValidationError;

    Dataset deleteDataSet(long dataSetId) throws ValidationError;

    List<DatasetChangeLogRegistered> getDatasetChangeLogs(long storageClusterId);

    Page<Dataset> getAllDatasetsByTypeAndSystem(final String datasetSubstring, final String storageTypeName,
            final String storageSystemName, final Pageable pageable);

    Dataset addDataset(Dataset dataset) throws ValidationError;

    Page<Dataset> getPendingDatasets(final String datasetSubstring, final String storageTypeName,
            final String storageSystemName, final Pageable pageable);

    void updateDatasetWithPendingAttributes(final DatasetWithAttributes dataset) throws ValidationError;

    DatasetWithAttributes getPendingDataset(final long dataSetId) throws ValidationError;

    Page<Dataset> getAllDeletedDatasetsByTypeAndSystem(final String datasetSubstring, String storageTypeName,
            String storageSystemName, Pageable pageable);

    long getDatasetCount();

    List<List<String>> getSampleData(String datasetName, long objectId) throws ValidationError;

    List<CumulativeDataset> getAllDetailedDatasets(String dataSetSubString);
}
