package com.paypal.udc.entity.objectschema;

public class Schema {

    private String columnName;
    private String columnType;
    private String columnFamily;
    private String columnClass;
    private boolean restrictionStatus;
    private boolean partitionStatus;
    private int columnIndex;

    public boolean isPartitionStatus() {
        return this.partitionStatus;
    }

    public void setPartitionStatus(final boolean partitionStatus) {
        this.partitionStatus = partitionStatus;
    }

    public int getColumnIndex() {
        return this.columnIndex;
    }

    public void setColumnIndex(final int columnIndex) {
        this.columnIndex = columnIndex;
    }

    public boolean isRestrictionStatus() {
        return this.restrictionStatus;
    }

    public void setRestrictionStatus(final boolean restrictionStatus) {
        this.restrictionStatus = restrictionStatus;
    }

    public String getColumnClass() {
        return this.columnClass == null ? "" : this.columnClass;
    }

    public void setColumnClass(final String columnClass) {
        this.columnClass = columnClass;
    }

    public String getColumnName() {
        return this.columnName;
    }

    public void setColumnName(final String columnName) {
        this.columnName = columnName;
    }

    public String getColumnType() {
        return this.columnType;
    }

    public void setColumnType(final String columnType) {
        this.columnType = columnType;
    }

    public String getColumnFamily() {
        return this.columnFamily;
    }

    public void setColumnFamily(final String columnFamily) {
        this.columnFamily = columnFamily;
    }
}
