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

import { Component, OnInit } from '@angular/core';
import { FormBuilder, FormGroup } from '@angular/forms';
import { MatDialogRef } from '@angular/material';
import { onValueChanged } from '../shared/utils';

@Component({
  selector: 'app-dataset-description-dialog',
  templateUrl: './dataset-description-dialog.component.html',
  styleUrls: ['./dataset-description-dialog.component.scss'],
})
export class DatasetDescriptionDialogComponent implements OnInit {

  heading = '';
  datasetDescriptionForm: FormGroup;
  datasetName: string;
  datasetDescription: string;

  constructor(public dialogRef: MatDialogRef<DatasetDescriptionDialogComponent>, private fb: FormBuilder) {
  }

  ngOnInit() {
    this.datasetDescriptionForm = this.fb.group({});
    this.heading = 'Description for ' + this.datasetName;
    this.datasetDescriptionForm.valueChanges.subscribe(data => onValueChanged(this.datasetDescriptionForm, {}, []));
  }

  cancel() {
    this.dialogRef.close();
  }

}
