/*
 * Copyright 2019 PayPal Inc.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.paypal.udc.entity.objectschema;

public class Schema {

    private String columnName;
    private String columnType;
    private String columnFamily;
    private String columnClass;
    private boolean restrictionStatus;
    private boolean partitionStatus;
    private int columnIndex;
    private int columnLength;
    private int columnPrecision;
    private int columnScale;

    public int getColumnScale() {
        return this.columnScale;
    }

    public void setColumnScale(final int columnScale) {
        this.columnScale = columnScale;
    }

    public int getColumnLength() {
        return this.columnLength;
    }

    public void setColumnLength(final int columnLength) {
        this.columnLength = columnLength;
    }

    public int getColumnPrecision() {
        return this.columnPrecision;
    }

    public void setColumnPrecision(final int columnPrecision) {
        this.columnPrecision = columnPrecision;
    }

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
