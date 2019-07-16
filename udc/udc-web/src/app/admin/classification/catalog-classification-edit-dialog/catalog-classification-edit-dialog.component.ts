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

import { Component, Input, OnInit, ViewChild } from '@angular/core';
import { FormGroup, FormBuilder, Validators, FormControl } from '@angular/forms';
import { MatDialogRef, MatStepper, MatStepperModule } from '@angular/material';
import { CustomValidators, onValueChanged } from '../../../shared/utils';
import { CatalogService } from '../../../udc/catalog/services/catalog.service';
import { Classification } from '../../models/catalog-classification';
import { SessionService } from '../../../core/services/session.service';
import { Page } from '../../../udc/catalog/models/catalog-list-page';
import { ConfigService } from '../../../core/services';

@Component({
  selector: 'app-catalog-classification-edit-dialog',
  templateUrl: './catalog-classification-edit-dialog.component.html',
  styleUrls: ['./catalog-classification-edit-dialog.component.scss'],
})

export class CatalogClassificationEditDialogComponent implements OnInit {
  heading = 'Edit Classification';
  editForm: FormGroup;
  maxCharsForName = 100;
  maxCharsForUserName = 20;
  maxCharsForDescName = 100;
  page = new Page();
  datasetClassificationId: number;
  objectName: string;
  columnName: string;
  classificationId: number;
  createdUser: string;
  updatedUser: string;
  classificationComment: string;
  public classificationCategories = [];
  public allMappedDatasets = Array<any>();
  public providerName = 'user';
  public selected = [];
  public datasetsLoaded = false;
  public username = '';
  public readonly nameHint = 'Valid characters are a-z,0-9 and -. Names should not start with -.';
  public readonly usernameHint = 'Valid characters are a-z.';
  private readonly regex = '^(([A-Za-z0-9]+\-)*[A-Za-z0-9_\-]+)*$';

  formErrors = {
    'classificationComment': '',
  };

  validationMessages = {
    'classificationComment': {
      'required': 'Comments required.',
      'maxlength': `comments cannot be more than ${this.maxCharsForDescName} characters long.`,
      'pattern': this.nameHint,
    },
  };

  constructor(public dialogRef: MatDialogRef<CatalogClassificationEditDialogComponent>, private fb: FormBuilder, private catalogService: CatalogService, private sessionService: SessionService, private config: ConfigService) {
    this.page.pageNumber = 0;
    this.page.size = 5;
    this.config.getUserNameEmitter().subscribe(data => {
      this.username = data;
    });
    this.username = this.config.userName;
  }

  ngOnInit() {
    this.classificationCategories = [0, 1, 2, 3, 4, 5];
    this.editForm = this.fb.group({
      'datasetClassificationId': '',
      'objectName': new FormControl({value: '', disabled: true}),
      'columnName': '',
      'classificationId': [],
      'classificationComment': ['', [CustomValidators.required, Validators.maxLength(this.maxCharsForDescName)]],
      'datasetIds': [],
    });

    this.editForm.valueChanges.subscribe(data => onValueChanged(this.editForm, this.formErrors, this.validationMessages));
    onValueChanged(this.editForm, this.formErrors, this.validationMessages);
  }

  cancel() {
    this.dialogRef.close();
  }

  onClassChange(event) {
    this.classificationId = event.value;
  }

  onSubmit() {
    const submitValue = Object.assign({}, this.editForm.value);
    const classification: Classification = this.populateClassification(submitValue);

    this.catalogService.editClassification(classification)
      .subscribe(result => {
        this.dialogRef.close({status: 'success', datasetClassificationId: result.datasetClassificationId});
      }, error => {
        if (error.status === 500) {
          this.dialogRef.close({status: 'fail', error: ''});
        } else {
          this.dialogRef.close({status: 'fail', error: error});
        }
      });
  }

  onSelect({selected}) {
    this.selected.splice(0, this.selected.length);
    this.selected.push(...selected);
  }

  private populateClassification(submitValue) {
    const selectedStorageDatasetIds = [];
    if (this.selected.length !== 0) {
      this.selected.forEach(element => {
        selectedStorageDatasetIds.push(element.storageDataSetId);
      });
    }
    const classification: Classification = new Classification(this.datasetClassificationId, this.classificationId, this.objectName, this.columnName, this.providerName, submitValue.classificationComment, this.createdUser, this.username, selectedStorageDatasetIds);
    return classification;
  }

  findDatasetMappings() {
    this.selected = [];
    this.catalogService.getDatastoreMapingList(this.objectName)
      .subscribe(data => {
        this.allMappedDatasets = data;
        this.datasetsLoaded = true;
      }, error => {
        this.dialogRef.close({status: 'fail', error: ''});
      });
  }
}
