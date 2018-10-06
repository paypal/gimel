package com.paypal.udc.entity.dataset;

import java.util.List;


public class CumulativeDataset {

    private long storageTypeId;
    private String storageTypeName;
    private List<Dataset> datasets;

    public CumulativeDataset(final long storageTypeId, final String storageTypeName, final List<Dataset> datasets) {
        this.storageTypeId = storageTypeId;
        this.storageTypeName = storageTypeName;
        this.datasets = datasets;
    }

    public long getStorageTypeId() {
        return this.storageTypeId;
    }

    public void setStorageTypeId(final long storageTypeId) {
        this.storageTypeId = storageTypeId;
    }

    public String getStorageTypeName() {
        return this.storageTypeName;
    }

    public void setStorageTypeName(final String storageTypeName) {
        this.storageTypeName = storageTypeName;
    }

    public List<Dataset> getDatasets() {
        return this.datasets;
    }

    public void setDatasets(final List<Dataset> datasets) {
        this.datasets = datasets;
    }

}
