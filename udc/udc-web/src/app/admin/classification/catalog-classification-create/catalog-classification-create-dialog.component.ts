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

import { Component, OnInit, ViewChild } from '@angular/core';
import { FormGroup, FormBuilder, Validators } from '@angular/forms';
import { MatDialogRef, MatStepper, MatStepperModule } from '@angular/material';
import { CustomValidators, onValueChanged } from '../../../shared/utils';
import { CatalogService } from '../../../udc/catalog/services/catalog.service';
import { Classification } from '../../models/catalog-classification';
import { SessionService } from '../../../core/services/session.service';
import { Page } from '../../../udc/catalog/models/catalog-list-page';
import { ConfigService } from '../../../core/services';

@Component({
  selector: 'app-catalog-classification-create-dialog',
  templateUrl: './catalog-classification-create-dialog.component.html',
  styleUrls: ['./catalog-classification-create-dialog.component.scss'],
})

export class CatalogCreateClassificationDialogComponent implements OnInit {
  heading = 'Create Classification';
  createForm: FormGroup;
  // maxCharsForName = 100;
  maxCharsForEntryDetails = 100;
  // maxCharsForDescName = 100;
  page = new Page();
  public classificationCategories = [];
  public allMappedDatasets = Array<any>();
  public providerName = 'user';
  public selected = [];
  public dbLoading = false;
  public datasetsLoaded = false;
  public username = '';
  public readonly nameHint = 'Valid characters are a-z,0-9 and -. Names should not start with -.';
  public readonly usernameHint = 'Valid characters are a-z.';
  private readonly regex = '^(([A-Za-z0-9]+\-)*[A-Za-z0-9_\-]+)*$';

  formErrors = {
    'objectName': '',
    'columnName': '',
    'comments': '',
  };

  validationMessages = {
    'objectName': {
      'required': 'Table name is required.',
      'maxlength': `name cannot be more than ${this.maxCharsForEntryDetails} characters long.`,
      'pattern': this.nameHint,
    }, 'columnName': {
      'required': 'Column name is required.',
      'maxlength': `name cannot be more than ${this.maxCharsForEntryDetails} characters long.`,
      'pattern': this.nameHint,
    }, 'comments': {
      'required': 'Comments required.',
      'maxlength': `comments cannot be more than ${this.maxCharsForEntryDetails} characters long.`,
      'pattern': this.nameHint,
    },
  };

  constructor(public dialogRef: MatDialogRef<CatalogCreateClassificationDialogComponent>, private fb: FormBuilder, private catalogService: CatalogService, private sessionService: SessionService, private config: ConfigService) {
    this.page.pageNumber = 0;
    this.page.size = 5;
    this.config.getUserNameEmitter().subscribe(data => {
      this.username = data;
    });
    this.username = this.config.userName;
  }

  ngOnInit() {
    this.classificationCategories = [0, 1, 2, 3, 4, 5];
    this.createForm = this.fb.group({
      'objectName': ['', [CustomValidators.required, Validators.maxLength(this.maxCharsForEntryDetails), Validators.pattern(this.regex)]],
      'columnName': ['', [CustomValidators.required, Validators.maxLength(this.maxCharsForEntryDetails), Validators.pattern(this.regex)]],
      'classificationId': [],
      'comments': ['', [CustomValidators.required, Validators.maxLength(this.maxCharsForEntryDetails)]],
      'datasetIds': [],
    });

    this.createForm.valueChanges.subscribe(data => onValueChanged(this.createForm, this.formErrors, this.validationMessages));
    onValueChanged(this.createForm, this.formErrors, this.validationMessages);
  }

  cancel() {
    this.dialogRef.close();
  }

  onSubmit() {
    const submitValue = Object.assign({}, this.createForm.value);
    const classification: Classification = this.populateClassification(submitValue);

    this.catalogService.insertClassification(classification)
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
    const classification: Classification = new Classification(0, submitValue.classificationId, submitValue.objectName, submitValue.columnName, this.providerName, submitValue.comments, this.username, this.username, selectedStorageDatasetIds);
    return classification;
  }

  findDatasetMappings() {
    this.selected = [];
    this.datasetsLoaded = false;
    this.catalogService.getDatastoreMapingList(this.createForm.value.objectName)
      .subscribe(data => {
        this.allMappedDatasets = data;
        this.datasetsLoaded = true;
      }, error => {
        this.dialogRef.close({status: 'fail', error: ''});
      });
  }

}
