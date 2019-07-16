package com.paypal.udc.entity.classification;

public class ClassificationDatasetMap {

    private long datasetClassificationId;
    private String objectName;
    private String columnName;
    private long providerId;
    private long classificationId;
    private String classificationComment;
    private String dataSource;
    private String dataSourceType;
    private String containerName;
    private long storageDatasetId;
    private long storageSystemId;
    private long entityId;
    private long zoneId;
    private String createdUser;
    private String createdTimestamp;
    private String updatedUser;
    private String updatedTimestamp;

    public long getClassificationId() {
        return this.classificationId;
    }

    public long getStorageDatasetId() {
        return this.storageDatasetId;
    }

    public void setStorageDatasetId(final long storageDatasetId) {
        this.storageDatasetId = storageDatasetId;
    }

    public long getStorageSystemId() {
        return this.storageSystemId;
    }

    public void setStorageSystemId(final long storageSystemId) {
        this.storageSystemId = storageSystemId;
    }

    public long getEntityId() {
        return this.entityId;
    }

    public void setEntityId(final long entityId) {
        this.entityId = entityId;
    }

    public long getZoneId() {
        return this.zoneId;
    }

    public void setZoneId(final long zoneId) {
        this.zoneId = zoneId;
    }

    public long getDatasetClassificationId() {
        return this.datasetClassificationId;
    }

    public void setDatasetClassificationId(final long datasetClassificationId) {
        this.datasetClassificationId = datasetClassificationId;
    }

    public String getObjectName() {
        return this.objectName;
    }

    public void setObjectName(final String objectName) {
        this.objectName = objectName;
    }

    public String getColumnName() {
        return this.columnName;
    }

    public void setColumnName(final String columnName) {
        this.columnName = columnName;
    }

    public long getProviderId() {
        return this.providerId;
    }

    public void setProviderId(final long providerId) {
        this.providerId = providerId;
    }

    public void setClassificationId(final long classificationId) {
        this.classificationId = classificationId;
    }

    public String getClassificationComment() {
        return this.classificationComment;
    }

    public void setClassificationComment(final String classificationComment) {
        this.classificationComment = classificationComment;
    }

    public String getDataSource() {
        return this.dataSource;
    }

    public void setDataSource(final String dataSource) {
        this.dataSource = dataSource;
    }

    public String getDataSourceType() {
        return this.dataSourceType;
    }

    public void setDataSourceType(final String dataSourceType) {
        this.dataSourceType = dataSourceType;
    }

    public String getContainerName() {
        return this.containerName;
    }

    public void setContainerName(final String containerName) {
        this.containerName = containerName;
    }

    public String getCreatedUser() {
        return this.createdUser;
    }

    public void setCreatedUser(final String createdUser) {
        this.createdUser = createdUser;
    }

    public String getCreatedTimestamp() {
        return this.createdTimestamp;
    }

    public void setCreatedTimestamp(final String createdTimestamp) {
        this.createdTimestamp = createdTimestamp;
    }

    public String getUpdatedUser() {
        return this.updatedUser;
    }

    public void setUpdatedUser(final String updatedUser) {
        this.updatedUser = updatedUser;
    }

    public String getUpdatedTimestamp() {
        return this.updatedTimestamp;
    }

    public void setUpdatedTimestamp(final String updatedTimestamp) {
        this.updatedTimestamp = updatedTimestamp;
    }

    public ClassificationDatasetMap() {

    }

    public ClassificationDatasetMap(final long datasetClassificationId, final String objectName,
            final String columnName, final long providerId, final long classificationId,
            final String classificationComment, final String dataSource, final String dataSourceType,
            final String containerName, final long storageDatasetId, final long storageSystemId, final long entityId,
            final long zoneId, final String createdUser, final String createdTimestamp, final String updatedUser,
            final String updatedTimestamp) {
        this.datasetClassificationId = datasetClassificationId;
        this.objectName = objectName;
        this.columnName = columnName;
        this.providerId = providerId;
        this.classificationId = classificationId;
        this.classificationComment = classificationComment;
        this.dataSource = dataSource;
        this.dataSourceType = dataSourceType;
        this.containerName = containerName;
        this.storageDatasetId = storageDatasetId;
        this.storageSystemId = storageSystemId;
        this.entityId = entityId;
        this.zoneId = zoneId;
        this.createdUser = createdUser;
        this.createdTimestamp = createdTimestamp;
        this.updatedUser = updatedUser;
        this.updatedTimestamp = updatedTimestamp;
    }

}
