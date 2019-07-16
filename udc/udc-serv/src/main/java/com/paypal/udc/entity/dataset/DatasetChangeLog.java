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

package com.paypal.udc.entity.dataset;

import java.io.Serializable;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;
import io.swagger.annotations.ApiModelProperty;


@Entity
@Table(name = "pc_storage_dataset_change_log")
public class DatasetChangeLog implements Serializable {

    private static final long serialVersionUID = 1L;

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @ApiModelProperty(notes = "The database generated change log ID")
    @Column(name = "storage_dataset_change_log_id")
    @NotNull
    private long storageDatasetChangeLogId;

    @ApiModelProperty(notes = "The database generated dataset ID")
    @Column(name = "storage_dataset_id")
    @NotNull
    private long storageDataSetId;

    @ApiModelProperty(notes = "Change Type")
    @Column(name = "storage_dataset_change_type")
    private String storageDatasetChangeType;

    @ApiModelProperty(notes = "Change Type")
    @Column(name = "change_column_type")
    private String changeColumnType;

    @ApiModelProperty(notes = "Change Column Previous Value")
    @Column(name = "change_column_prev_val", columnDefinition = "json")
    private String changeColumnPrevValInString;

    @ApiModelProperty(notes = "Change Column Current Value")
    @Column(name = "change_column_curr_val", columnDefinition = "json")
    private String changeColumnCurrValInString;

    @ApiModelProperty(notes = "Updated Timestamp")
    @Column(name = "upd_ts")
    private String updatedTimestamp;

    public DatasetChangeLog() {

    }

    @Override
    public String toString() {
        return "DatasetChangeLog [storageDatasetChangeLogId=" + this.storageDatasetChangeLogId + ", storageDataSetId="
                + this.storageDataSetId + ", storageDatasetChangeType=" + this.storageDatasetChangeType
                + ", changeColumnType=" + this.changeColumnType + ", changeColumnPrevValInString="
                + this.changeColumnPrevValInString + ", changeColumnCurrValInString=" + this.changeColumnCurrValInString
                + ", updatedTimestamp=" + this.updatedTimestamp + "]";
    }

    public DatasetChangeLog(final long storageDataSetId, final String storageDatasetChangeType,
            final String changeColumnType, final String changeColumnPrevValInString,
            final String changeColumnCurrValInString, final String updatedTimestamp) {
        this.storageDataSetId = storageDataSetId;
        this.storageDatasetChangeType = storageDatasetChangeType;
        this.changeColumnType = changeColumnType;
        this.changeColumnPrevValInString = changeColumnPrevValInString;
        this.changeColumnCurrValInString = changeColumnCurrValInString;
        this.updatedTimestamp = updatedTimestamp;
    }

    public long getStorageDatasetChangeLogId() {
        return this.storageDatasetChangeLogId;
    }

    public void setStorageDatasetChangeLogId(final long storageDatasetChangeLogId) {
        this.storageDatasetChangeLogId = storageDatasetChangeLogId;
    }

    public long getStorageDataSetId() {
        return this.storageDataSetId;
    }

    public void setStorageDataSetId(final long storageDataSetId) {
        this.storageDataSetId = storageDataSetId;
    }

    public String getStorageDatasetChangeType() {
        return this.storageDatasetChangeType;
    }

    public void setStorageDatasetChangeType(final String storageDatasetChangeType) {
        this.storageDatasetChangeType = storageDatasetChangeType;
    }

    public String getChangeColumnType() {
        return this.changeColumnType;
    }

    public void setChangeColumnType(final String changeColumnType) {
        this.changeColumnType = changeColumnType;
    }

    public String getColumnPrevValInString() {
        return this.changeColumnPrevValInString;
    }

    public void setColumnPrevValInString(final String changeColumnPrevValInString) {
        this.changeColumnPrevValInString = changeColumnPrevValInString;
    }

    public String getColumnCurrValInString() {
        return this.changeColumnCurrValInString;
    }

    public void setColumnCurrValInString(final String changeColumnCurrValInString) {
        this.changeColumnCurrValInString = changeColumnCurrValInString;
    }

    public String getUpdatedTimestamp() {
        return this.updatedTimestamp;
    }

    public void setUpdatedTimestamp(final String updatedTimestamp) {
        this.updatedTimestamp = updatedTimestamp;
    }

}
