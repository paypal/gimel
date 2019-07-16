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

import {Component, OnInit} from '@angular/core';
import {FormGroup, FormBuilder, Validators} from '@angular/forms';
import {MatDialogRef} from '@angular/material';
import {CustomValidators, onValueChanged} from '../../../shared/utils';
import {CatalogService} from '../../../udc/catalog/services/catalog.service';
import {Type} from '../../models/catalog-type';
import {System} from '../../models/catalog-system';

@Component({
  selector: 'app-catalog-system-edit-dialog', templateUrl: './catalog-system-edit-dialog.component.html', styleUrls: ['./catalog-system-edit-dialog.component.scss'],
})

export class CatalogSystemEditDialogComponent implements OnInit {
  heading = 'Edit System';
  editSystemForm: FormGroup;
  maxCharsForName = 100;
  maxCharsForUserName = 20;
  maxCharsForAliasName = 100;
  maxCharsForDescName = 100;
  storageSystemId: number;
  storageSystemName: string;
  adminUserId: number;
  storageSystemDescription: string;
  createdUser: string;
  containers: string;
  systemAttributes: Array<any>;
  discoverySla: string;
  editing = {};
  public compatibilityList = Array<string>();
  public readonly nameHint = 'Valid characters are a-z,0-9 and -. Names should not start with -.';
  public readonly usernameHint = 'Valid characters are a-z.';
  private readonly regex = '^(([a-z0-9]+\-)*[a-z0-9]+)*$';

  formErrors = {
    'modifiedStorageSystemName': '', 'updatedUser': '', 'modifiedStorageSystemDescription': '', 'modifiedContainers': '', 'modifiedSla': '',
  };

  validationMessages = {
    'modifiedStorageSystemName': {
      'required': 'Storage System name is required.', 'maxlength': `name cannot be more than ${ this.maxCharsForName } characters long.`, 'pattern': this.nameHint,
    }, 'updatedUser': {
      'required': 'user name is required.', 'maxlength': `name cannot be more than ${ this.maxCharsForUserName } characters long.`, 'pattern': this.usernameHint,
    }, 'modifiedStorageSystemDescription': {
      'required': 'Storage System description is required.', 'maxlength': `name cannot be more than ${ this.maxCharsForDescName } characters long.`, 'pattern': this.nameHint,
    }, 'modifiedContainers': {
      'required': 'Storage System name is required.', 'maxlength': `name cannot be more than ${ this.maxCharsForName } characters long.`, 'pattern': this.nameHint,
    }, 'modifiedSla': [],
  };

  constructor(public dialogRef: MatDialogRef<CatalogSystemEditDialogComponent>, private fb: FormBuilder, private catalogService: CatalogService) {
  }

  ngOnInit() {
    this.compatibilityList.push('Y');
    this.compatibilityList.push('N');
    this.editSystemForm = this.fb.group({
      'modifiedStorageSystemName': ['', [Validators.maxLength(this.maxCharsForName)]], 'modifiedContainers': ['', [Validators.maxLength(this.maxCharsForName)]], 'modifiedStorageSystemDescription': ['', [Validators.maxLength(this.maxCharsForAliasName)]], 'isReadCompatible': [], 'updatedUser': ['', [CustomValidators.required, Validators.maxLength(this.maxCharsForUserName), Validators.pattern(this.regex)]],
    });

    this.editSystemForm.valueChanges.subscribe(data => onValueChanged(this.editSystemForm, this.formErrors, this.validationMessages));
    onValueChanged(this.editSystemForm, this.formErrors, this.validationMessages);
  }

  cancel() {
    this.dialogRef.close();
  }

  populateSystem(submitValue) {
    const data: System = new System();
    data.storageSystemId = this.storageSystemId;
    if (submitValue.modifiedStorageSystemName.length > 0) {
      data.storageSystemName = submitValue.modifiedStorageSystemName;
    } else {
      data.storageSystemName = this.storageSystemName;
    }
    if (submitValue.modifiedContainers.length > 0) {
      data.containers = submitValue.modifiedContainers;
    } else {
      data.containers = this.containers;
    }
    if (submitValue.modifiedStorageSystemDescription.length > 0) {
      data.storageSystemDescription = submitValue.modifiedStorageSystemDescription;
    } else {
      data.storageSystemDescription = this.storageSystemDescription;
    }

    data.discoverySla = this.discoverySla;
    data.isReadCompatible = submitValue.isReadCompatible;
    data.updatedUser = submitValue.updatedUser;
    data.systemAttributeValues = this.systemAttributes;
    return data;
  }

  onSubmit() {
    const submitValue = Object.assign({}, this.editSystemForm.value);
    const system: System = this.populateSystem(submitValue);
    this.catalogService.getUserByName(system.updatedUser)
      .subscribe(data => {
        this.catalogService.updateSystem(system)
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

  updateValue(event, cell, rowIndex) {
    this.editing[rowIndex + '-' + cell] = false;
    this.systemAttributes[rowIndex][cell] = event.target.value;
    this.systemAttributes = [...this.systemAttributes];

  }
}
