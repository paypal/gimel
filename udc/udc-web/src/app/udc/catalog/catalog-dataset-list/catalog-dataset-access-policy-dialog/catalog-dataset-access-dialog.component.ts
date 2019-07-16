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
import { FormGroup, FormBuilder } from '@angular/forms';
import { MatDialogRef } from '@angular/material';
import { TeradataPolicy } from '../../models/catalog-teradata-policy';
import { DerivedPolicy } from '../../models/catalog-derived-policy';

@Component({
  selector: 'app-catalog-access-dialog', templateUrl: './catalog-dataset-access-dialog.component.html', styleUrls: ['./catalog-dataset-access-dialog.component.scss'],
})

export class CatalogDatasetAccessDialogComponent implements OnInit {
  heading = '';
  policyForm: FormGroup;
  public datasetName: string;
  public accessControlList = [];
  public typeName: string;

  constructor(public dialogRef: MatDialogRef<CatalogDatasetAccessDialogComponent>, private fb: FormBuilder) {
  }

  ngOnInit() {
    this.policyForm = this.fb.group({});
    this.heading = 'Access Control Policies for ' + this.datasetName;
  }

  cancel() {
    this.dialogRef.close();
  }
}
