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
import { FormGroup, FormBuilder, Validators } from '@angular/forms';
import { MatDialogRef } from '@angular/material';
import { CustomValidators, onValueChanged } from '../../../shared/utils';
import { CatalogService } from '../../../udc/catalog/services/catalog.service';

@Component({
  selector: 'app-catalog-type-view-attributes-dialog',
  templateUrl: './catalog-type-view-attributes-dialog.component.html',
  styleUrls: ['./catalog-type-view-attributes-dialog.component.scss'],
})

export class CatalogTypeViewAttributesDialogComponent implements OnInit {
  heading = '';
  editTypeForm: FormGroup;
  storageTypeId: number;
  storageTypeName: string;
  typeAttributes: Array<any>;
  editing = {};

  constructor(public dialogRef: MatDialogRef<CatalogTypeViewAttributesDialogComponent>, private fb: FormBuilder, private catalogService: CatalogService) {
  }

  ngOnInit() {
    this.editTypeForm = this.fb.group({
    });
    this.heading = 'Attributes for ' + this.storageTypeName;
    this.editTypeForm.valueChanges.subscribe(data => onValueChanged(this.editTypeForm, {}, []));
  }

  cancel() {
    this.dialogRef.close();
  }
}
