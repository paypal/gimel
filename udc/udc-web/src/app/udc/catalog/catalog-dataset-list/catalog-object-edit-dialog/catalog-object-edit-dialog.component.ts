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
import {FormControl, FormGroup, FormBuilder, Validators} from '@angular/forms';
import {MatDialogRef} from '@angular/material';
import {CustomValidators, onValueChanged} from '../../../../shared/utils';
import {CatalogService} from '../../services/catalog.service';
import {ObjectSchemaMap} from '../../models/catalog-objectschema';
import {ObjectAttributeKeyValue} from '../../models/catalog-object-attribute-key-value';

@Component({
  selector: 'app-catalog-object-edit-dialog', templateUrl: './catalog-object-edit-dialog.component.html', styleUrls: ['./catalog-object-edit-dialog.component.scss'],
})

export class CatalogObjectEditDialogComponent implements OnInit {
  heading = 'Editing Object for ';
  editForm: FormGroup;
  maxCharsForName = 100;
  maxCharsForUserName = 20;
  maxCharsForAliasName = 100;
  maxCharsForDescName = 100;
  objectId: number;
  objectName: string;
  containerName: string;
  storageSystemId: number;
  createdUser: string;
  objectSchema: Array<any> = new Array<any>();
  objectAttributes: Array<any> = new Array<any>();
  customAttributes: Array<ObjectAttributeKeyValue> = new Array<ObjectAttributeKeyValue>();
  editing = {};
  public readonly nameHint = 'Valid characters are a-z,0-9 and -. Names should not start with -.';
  public readonly usernameHint = 'Valid characters are a-z.';
  private readonly regex = '^(([a-z0-9]+\-)*[a-z0-9]+)*$';

  formErrors = {
    'updatedUser': '',
  };

  validationMessages = {
    'updatedUser': {
      'required': 'user name is required.', 'maxlength': `name cannot be more than ${ this.maxCharsForUserName } characters long.`, 'pattern': this.usernameHint,
    },
  };

  constructor(public dialogRef: MatDialogRef<CatalogObjectEditDialogComponent>, private fb: FormBuilder, private catalogService: CatalogService) {
  }

  ngOnInit() {
    this.heading += this.objectName;
    this.editForm = this.fb.group({
      'updatedUser': ['', [CustomValidators.required, Validators.maxLength(this.maxCharsForUserName), Validators.pattern(this.regex)]],
    });
    this.editForm.valueChanges.subscribe(data => onValueChanged(this.editForm, this.formErrors, this.validationMessages));
    onValueChanged(this.editForm, this.formErrors, this.validationMessages);
  }

  cancel() {
    this.dialogRef.close();
  }

  onSubmit() {
    const submitValue = Object.assign({}, this.editForm.value);
    const object: ObjectSchemaMap = new ObjectSchemaMap(this.objectId, this.objectName, this.containerName, this.storageSystemId, submitValue.updatedUser);
    object.objectSchema = this.objectSchema;
    object.objectAttributes = this.objectAttributes;
    this.catalogService.getUserByName(submitValue.updatedUser)
      .subscribe(data => {
        this.catalogService.updateObject(object)
          .subscribe(objectResult => {
            if (this.customAttributes) {
              this.customAttributes.forEach(customAttribute => {
                this.catalogService.updateCustomAttributes(customAttribute)
                  .subscribe(attributeResult => {
                    this.dialogRef.close({status: 'success', objectId: this.objectId});
                  }, error => {
                    if (error.status === 500) {
                      this.dialogRef.close({status: 'fail', error: ''});
                    } else {
                      this.dialogRef.close({status: 'fail', error: error});
                    }
                  });
              });
            } else {
              this.dialogRef.close({status: 'success', objectId: this.objectId});
            }
            this.dialogRef.close({status: 'success', objectId: this.objectId});
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

  updateObjectAttributesValue(event, cell, rowIndex) {
    this.editing[rowIndex + '-' + cell] = false;
    this.objectAttributes[rowIndex][cell] = event.target.value;
    this.objectAttributes = [...this.objectAttributes];
  }

  updateCustomAttributesValue(event, cell, rowIndex) {
    this.editing[rowIndex + '-' + cell] = false;
    this.customAttributes[rowIndex][cell] = event.target.value;
    this.customAttributes = [...this.customAttributes];
  }
}
