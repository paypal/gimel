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
import { Entity } from '../../models/catalog-entity';

@Component({
  selector: 'app-catalog-entity-edit-dialog',
  templateUrl: './catalog-entity-edit-dialog.component.html',
  styleUrls: ['./catalog-entity-edit-dialog.component.scss'],
})

export class CatalogEntityEditDialogComponent implements OnInit {
  heading = 'Edit Entity';
  editEntityForm: FormGroup;
  maxCharsForName = 100;
  maxCharsForUserName = 20;
  maxCharsForAliasName = 100;
  maxCharsForDescName = 100;
  entityId: number;
  entityName: string;
  entityDescription: string;
  createdUser: string;
  public readonly nameHint = 'Valid characters are a-z,0-9 and -. Names should not start with -.';
  public readonly usernameHint = 'Valid characters are a-z.';
  private readonly regex = '^(([a-z0-9]+\-)*[a-z0-9]+)*$';

  formErrors = {
    'modifiedEntityName': '',
    'updatedUser': '',
    'modifiedEntityDescription': '',
  };

  validationMessages = {
    'modifiedEntityName': {
      'required': 'Entity name is required.',
      'maxlength': `name cannot be more than ${ this.maxCharsForName } characters long.`,
      'pattern': this.nameHint,
    }, 'updatedUser': {
      'required': 'user name is required.',
      'maxlength': `name cannot be more than ${ this.maxCharsForUserName } characters long.`,
      'pattern': this.usernameHint,
    }, 'modifiedEntityDescription': {
      'required': 'Entity description is required.',
      'maxlength': `name cannot be more than ${ this.maxCharsForDescName } characters long.`,
      'pattern': this.nameHint,
    },
  };

  constructor(public dialogRef: MatDialogRef<CatalogEntityEditDialogComponent>, private fb: FormBuilder, private catalogService: CatalogService) {
  }

  ngOnInit() {
    this.editEntityForm = this.fb.group({
      'modifiedEntityName': ['', [Validators.maxLength(this.maxCharsForName)]],
      'modifiedEntityDescription': ['', [Validators.maxLength(this.maxCharsForDescName)]],
      'updatedUser': ['', [CustomValidators.required, Validators.maxLength(this.maxCharsForUserName), Validators.pattern(this.regex)]],
    });

    this.editEntityForm.valueChanges.subscribe(data => onValueChanged(this.editEntityForm, this.formErrors, this.validationMessages));
    onValueChanged(this.editEntityForm, this.formErrors, this.validationMessages);
  }

  cancel() {
    this.dialogRef.close();
  }

  populateEntity(submitValue) {
    const data: Entity = new Entity();
    data.entityId = this.entityId;
    data.createdUser = this.createdUser;
    if (submitValue.modifiedEntityName.length > 0) {
      data.entityName = submitValue.modifiedEntityName;
    } else {
      data.entityName = this.entityName;
    }
    if (submitValue.modifiedEntityDescription.length > 0) {
      data.entityDescription = submitValue.modifiedEntityDescription;
    } else {
      data.entityDescription = this.entityDescription;
    }
    data.updatedUser = submitValue.updatedUser;
    return data;
  }

  onSubmit() {
    const submitValue = Object.assign({}, this.editEntityForm.value);
    const entity: Entity = this.populateEntity(submitValue);
    this.catalogService.getUserByName(entity.updatedUser)
      .subscribe(data => {
        this.catalogService.updateEntity(entity)
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
