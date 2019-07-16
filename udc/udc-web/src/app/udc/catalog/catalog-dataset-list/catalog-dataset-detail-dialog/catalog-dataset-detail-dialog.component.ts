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

import {Component, Inject, OnInit} from '@angular/core';
import {FormBuilder, FormGroup} from "@angular/forms";
import {MAT_DIALOG_DATA, MatDialogRef} from "@angular/material";
import {onValueChanged} from "../../../../shared/utils";

@Component({
  selector: 'app-catalog-dataset-detail-dialog',
  templateUrl: './catalog-dataset-detail-dialog.component.html',
  styleUrls: ['./catalog-dataset-detail-dialog.component.scss']
})
export class CatalogDatasetDetailDialogComponent implements OnInit {

  heading = '';
  datasetDescriptionForm: FormGroup;
  storageDataSetName: string;
  storageDataSetId: number;

  constructor(public dialogRef: MatDialogRef<CatalogDatasetDetailDialogComponent>, private fb: FormBuilder, @Inject(MAT_DIALOG_DATA) data) {
    this.storageDataSetId = this.storageDataSetId;
    this.storageDataSetName = this.storageDataSetName;
  }

  ngOnInit() {
    this.datasetDescriptionForm = this.fb.group({});
    this.heading = 'Dataset Details';
    this.datasetDescriptionForm.valueChanges.subscribe(data => onValueChanged(this.datasetDescriptionForm, {}, []));
  }

  cancel() {
    this.dialogRef.close();
  }
}
