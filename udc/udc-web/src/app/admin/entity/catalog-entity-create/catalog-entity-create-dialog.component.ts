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
  selector: 'app-catalog-entity-create-dialog',
  templateUrl: './catalog-entity-create-dialog.component.html',
  styleUrls: ['./catalog-entity-create-dialog.component.scss'],
})

export class CatalogCreateEntityDialogComponent implements OnInit {
  heading = 'Create Entity';
  createForm: FormGroup;
  maxCharsForName = 100;
  maxCharsForUserName = 20;
  maxCharsForAliasName = 100;
  maxCharsForDescName = 100;
  createdUser: string;
  public readonly nameHint = 'Valid characters are a-z,0-9 and -. Names should not start with -.';
  public readonly usernameHint = 'Valid characters are a-z.';
  private readonly regex = '^(([a-z0-9]+\-)*[a-z0-9]+)*$';

  formErrors = {
    'entityName': '', 'entityDescription': '', 'createdUser': '',
  };

  validationMessages = {
    'entityName': {
      'required': 'Entity name is required.',
      'maxlength': `name cannot be more than ${ this.maxCharsForName } characters long.`,
      'pattern': this.nameHint,
    }, 'createdUser': {
      'required': 'username is required.',
      'maxlength': `name cannot be more than ${ this.maxCharsForUserName } characters long.`,
      'pattern': this.usernameHint,
    }, 'entityDescription': {
      'required': 'Entity description is required.',
      'maxlength': `name cannot be more than ${ this.maxCharsForDescName } characters long.`,
      'pattern': this.nameHint,
    },
  };

  constructor(public dialogRef: MatDialogRef<CatalogCreateEntityDialogComponent>, private fb: FormBuilder, private catalogService: CatalogService) {
  }

  ngOnInit() {
    this.createForm = this.fb.group({
      'entityName': ['', [CustomValidators.required, Validators.maxLength(this.maxCharsForName), Validators.pattern(this.regex)]],
      'entityDescription': ['', [CustomValidators.required, Validators.maxLength(this.maxCharsForDescName)]],
      'createdUser': ['', [CustomValidators.required, Validators.maxLength(this.maxCharsForUserName), Validators.pattern(this.regex)]],
    });

    this.createForm.valueChanges.subscribe(data => onValueChanged(this.createForm, this.formErrors, this.validationMessages));
    onValueChanged(this.createForm, this.formErrors, this.validationMessages);
  }

  cancel() {
    this.dialogRef.close();
  }

  onSubmit() {
    const submitValue = Object.assign({}, this.createForm.value);
    const entity: Entity = this.populateEntity(submitValue);
    this.catalogService.getUserByName(entity.createdUser)
      .subscribe(data => {
        this.catalogService.insertEntity(entity)
          .subscribe(result => {
            this.dialogRef.close({status: 'success', entityId: result.entityId});
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

  private populateEntity(submitValue) {
    const entity: Entity = new Entity();
    entity.entityDescription = submitValue.entityDescription;
    entity.createdUser = submitValue.createdUser;
    entity.entityName = submitValue.entityName;
    return entity;
  }
}
