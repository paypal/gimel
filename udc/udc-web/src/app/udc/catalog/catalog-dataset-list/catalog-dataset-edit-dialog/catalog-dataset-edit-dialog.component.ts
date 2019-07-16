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
import { Response } from '@angular/http';

import { FormControl, FormGroup, FormBuilder, Validators } from '@angular/forms';
import { MatDialogRef } from '@angular/material';
import { Dataset } from '../../models/catalog-dataset';
import { CustomValidators, onValueChanged } from '../../../../shared/utils';
import { CatalogService } from '../../services/catalog.service';
import { isNullOrUndefined } from 'util';

@Component({
  selector: 'app-catalog-dataset-edit-dialog',
  templateUrl: './catalog-dataset-edit-dialog.component.html',
  styleUrls: ['./catalog-dataset-edit-dialog.component.scss'],
})

export class CatalogDatasetEditDialogComponent implements OnInit {
  heading = 'Edit Dataset';
  editForm: FormGroup;
  maxCharsForName = 100;
  maxCharsForUserName = 20;
  maxCharsForAliasName = 100;
  maxCharsForDescName = 100;
  projectName: string;
  storageDataSetId: number;
  storageDataSetName: string;
  storageDataSetAliasName: string;
  storageDataSetDescription: string;
  createdUser: string;
  public readonly nameHint = 'Valid characters are a-z,0-9 and -. Names should not start with -.';
  public readonly usernameHint = 'Valid characters are a-z.';
  private readonly regex = '^(([a-z0-9]+\-)*[a-z0-9]+)*$';

  formErrors = {
    'modifiedDataSetName': '', 'updatedUser': '', 'modifiedDataSetAliasName': '', 'modifiedDataSetDescription': '',
  };

  validationMessages = {
    'modifiedDataSetName': {
      'required': 'Dataset name is required.',
      'maxlength': `name cannot be more than ${ this.maxCharsForName } characters long.`,
      'pattern': this.nameHint,
    }, 'updatedUser': {
      'required': 'user name is required.',
      'maxlength': `name cannot be more than ${ this.maxCharsForUserName } characters long.`,
      'pattern': this.usernameHint,
    }, 'modifiedDataSetAliasName': {
      'required': 'dataset alias is required.',
      'maxlength': `name cannot be more than ${ this.maxCharsForAliasName } characters long.`,
      'pattern': this.usernameHint,
    }, 'modifiedDataSetDescription': {
      'required': 'dataset description is required.',
      'maxlength': `name cannot be more than ${ this.maxCharsForDescName } characters long.`,
      'pattern': this.nameHint,
    },
  };

  constructor(public dialogRef: MatDialogRef<CatalogDatasetEditDialogComponent>, private fb: FormBuilder, private catalogService: CatalogService) {
  }

  ngOnInit() {
    this.editForm = this.fb.group({
      'modifiedDataSetName': ['', [Validators.maxLength(this.maxCharsForName)]],
      'modifiedDataSetAliasName': ['', [Validators.maxLength(this.maxCharsForAliasName)]],
      'modifiedDataSetDescription': ['', [Validators.maxLength(this.maxCharsForAliasName)]],
      'updatedUser': ['', [CustomValidators.required, Validators.maxLength(this.maxCharsForUserName), Validators.pattern(this.regex)]],
    });

    this.editForm.valueChanges.subscribe(data => onValueChanged(this.editForm, this.formErrors, this.validationMessages));
    onValueChanged(this.editForm, this.formErrors, this.validationMessages);
  }

  cancel() {
    this.dialogRef.close();
  }

  populateDataset(submitValue) {
    const data: Dataset = new Dataset(this.storageDataSetName, this.storageDataSetId, '', null, 0, '', '', '', '', '', '', [], [], '', '', '');
    data.storageDataSetId = this.storageDataSetId;
    if (submitValue.modifiedDataSetName.length > 0) {
      data.storageDataSetName = submitValue.modifiedDataSetName;
    } else {
      data.storageDataSetName = this.storageDataSetName;
    }
    if (submitValue.modifiedDataSetDescription.length > 0) {
      data.storageDataSetDescription = submitValue.modifiedDataSetDescription;
    } else {
      data.storageDataSetDescription = this.storageDataSetDescription;
    }
    if (submitValue.modifiedDataSetAliasName.length > 0) {
      data.storageDataSetAliasName = submitValue.modifiedDataSetAliasName;
    } else {
      data.storageDataSetAliasName = this.storageDataSetAliasName;
    }
    data.createdUser = submitValue.updatedUser;
    return data;
  }

  onSubmit() {
    const submitValue = Object.assign({}, this.editForm.value);
    const dataset: Dataset = this.populateDataset(submitValue);
    this.catalogService.getUserByName(submitValue.updatedUser)
      .subscribe(data => {
        this.catalogService.updateDataset(dataset)
          .subscribe(result => {
            this.dialogRef.close({status: 'success'});
          }, error => {
            if (error.status === 500) {
              this.dialogRef.close({status: 'fail', error: ''});
            } else {
              this.dialogRef.close({status: 'fail', error: error});
            }
          });
      }, error => {
        this.dialogRef.close({status: 'user fail', error: 'Invalid Username'});
      });
  }
}
