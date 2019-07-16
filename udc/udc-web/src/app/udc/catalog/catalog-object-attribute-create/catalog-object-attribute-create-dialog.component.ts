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
import { MatDialog } from '@angular/material';
import { ObjectAttributeKeyValue } from '../models/catalog-object-attribute-key-value';
import { environment } from '../../../../environments/environment';
import {SessionService} from '../../../core/services/session.service';

@Component({
  selector: 'app-catalog-object-attribute-create-dialog', templateUrl: './catalog-object-attribute-create-dialog.component.html', styleUrls: ['./catalog-object-attribute-create-dialog.component.scss'],
})

export class CatalogCreateObjectAttributeDialogComponent implements OnInit {
  heading = 'Create Custom Object Attribute for ';
  createForm: FormGroup;
  maxCharsForAttributeKey = 100;
  maxCharsForUserName = 20;
  maxCharsForAttributeValue = 10000;
  createdUser: string;
  objectName: string;
  objectId: number;
  public readonly nameHint = 'Valid characters are a-z,0-9 and -. Names should not start with -.';
  public readonly usernameHint = 'Valid characters are a-z.';
  private readonly regex = '^(([A-Za-z0-9]+\_)*[A-Za-z0-9]+)*$';
  formErrors = {
    'attributeKey': '', 'attributeValue': '', 'createdUser': '',
  };
  validationMessages = {
    'attributeKey': {
      'required': 'Attribute key is required.', 'maxlength': `name cannot be more than ${ this.maxCharsForAttributeKey } characters long.`, 'pattern': this.nameHint,
    }, 'createdUser': {
      'required': 'username is required.', 'maxlength': `name cannot be more than ${ this.maxCharsForUserName } characters long.`, 'pattern': this.usernameHint,
    }, 'attributeValue': {
      'required': 'Attribute Value is required.', 'maxlength': `name cannot be more than ${ this.maxCharsForAttributeValue } characters long.`, 'pattern': this.nameHint,
    },
  };

  constructor(public dialogRef: MatDialogRef<CatalogCreateObjectAttributeDialogComponent>, private dialog: MatDialog, private fb: FormBuilder, private catalogService: CatalogService, private sessionService: SessionService) {
  }

  ngOnInit() {
    this.createForm = this.fb.group({
      'attributeKey': ['', [CustomValidators.required, Validators.maxLength(this.maxCharsForAttributeKey)]], 'attributeValue': ['', [CustomValidators.required, Validators.maxLength(this.maxCharsForAttributeValue)]], 'createdUser': ['', [CustomValidators.required, Validators.maxLength(this.maxCharsForUserName), Validators.pattern(this.regex)]],
    });

    this.createForm.valueChanges.subscribe(data => onValueChanged(this.createForm, this.formErrors, this.validationMessages));
    onValueChanged(this.createForm, this.formErrors, this.validationMessages);
    if (this.objectName) {
      this.heading += this.objectName;
    } else {
      this.getObjectDetails(this.objectId);
    }
  }

  getObjectDetails(objectId: number) {
    this.catalogService.getObjectDetails(objectId.toString())
      .subscribe(data => {
        this.objectName = data.objectName;
        this.heading += this.objectName;
      }, error => {
      });
  }

  cancel() {
    this.dialogRef.close();
  }

  onSubmit() {
    const submitValue = Object.assign({}, this.createForm.value);
    const objectAttribute: ObjectAttributeKeyValue = this.populateObjectAttribute(submitValue);
    this.catalogService.getUserByName(objectAttribute.createdUser)
      .subscribe(userOutput => {
        this.catalogService.insertObjectAttribute(objectAttribute)
          .subscribe(attributeOutput => {
            this.dialogRef.close({
              status: 'success', objectAttributeKeyValueId: attributeOutput.objectAttributeKeyValueId,
            });
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

  populateObjectAttribute(result: any) {
    const attribute: ObjectAttributeKeyValue = new ObjectAttributeKeyValue();
    attribute.objectId = this.objectId;
    attribute.objectAttributeKey = result.attributeKey;
    attribute.objectAttributeValue = result.attributeValue;
    attribute.createdUser = result.createdUser;
    return attribute;
  }
}
