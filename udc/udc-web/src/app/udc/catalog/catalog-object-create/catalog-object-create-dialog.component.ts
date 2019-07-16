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
import { MatDialog, MatOptionSelectionChange, MatSnackBar } from '@angular/material';
import { System } from '../../../admin/models/catalog-system';
import { ConfigService } from '../../../core/services/config.service';
import { ObjectSchemaMap } from '../models/catalog-objectschema';
import { Dataset } from '../models/catalog-dataset';
import { StorageSystem } from '../models/catalog-storagesystem';
import { environment } from '../../../../environments/environment';
import {SessionService} from '../../../core/services/session.service';

@Component({
  selector: 'app-catalog-object-create-dialog', templateUrl: './catalog-object-create-dialog.component.html', styleUrls: ['./catalog-object-create-dialog.component.scss'],
})

export class CatalogCreateObjectDialogComponent implements OnInit {
  heading = 'Create Object';
  createForm: FormGroup;
  maxCharsForName = 100;
  maxCharsForUserName = 20;
  maxCharsForDescName = 1000;
  createdUser: string;
  public storageSystems = [];
  public clusterList = [];
  public typeAttributes = Array<any>();
  public dbLoading = false;
  rows = [];
  systemId: number;
  editing = {};
  public options = Array<string>();
  public wantToRegister: boolean;
  public user: any;
  userId: number;
  userName: string;
  storageSystem: StorageSystem;
  systemName: string;
  public readonly nameHint = 'Valid characters are a-z,0-9 and -. Names should not start with -.';
  public readonly usernameHint = 'Valid characters are a-z.';
  private readonly regex = '^(([A-Za-z0-9]+\_)*[A-Za-z0-9]+)*$';
  formErrors = {
    'objectName': '', 'containerName': '', 'createdUser': '',
  };

  validationMessages = {
    'objectName': {
      'required': 'Object Name is required.', 'maxlength': `name cannot be more than ${ this.maxCharsForName } characters long.`, 'pattern': this.nameHint,
    }, 'createdUser': {
      'required': 'username is required.', 'maxlength': `name cannot be more than ${ this.maxCharsForUserName } characters long.`, 'pattern': this.usernameHint,
    }, 'containerName': {
      'required': 'Container Name is required.', 'maxlength': `name cannot be more than ${ this.maxCharsForDescName } characters long.`, 'pattern': this.nameHint,
    },
  };

  constructor(private sessionService: SessionService, private config: ConfigService, public dialogRef: MatDialogRef<CatalogCreateObjectDialogComponent>, private snackbar: MatSnackBar, private dialog: MatDialog, private fb: FormBuilder, private catalogService: CatalogService) {
    this.options.push('Yes');
    this.options.push('No');
  }

  ngOnInit() {
    this.loadStorageSystems();
    this.loadClusterList();
    this.createForm = this.fb.group({
      'objectName': ['', [CustomValidators.required, Validators.maxLength(this.maxCharsForName), Validators.pattern(this.regex)]], 'containerName': ['', [CustomValidators.required, Validators.maxLength(this.maxCharsForDescName)]], 'createdUser': ['', [CustomValidators.required, Validators.maxLength(this.maxCharsForUserName), Validators.pattern(this.regex)]], 'storageSystem': [], 'wantToRegister': [],
    });

    this.createForm.valueChanges.subscribe(data => onValueChanged(this.createForm, this.formErrors, this.validationMessages));
    onValueChanged(this.createForm, this.formErrors, this.validationMessages);
  }

  cancel() {
    this.dialogRef.close();
  }

  loadClusterList() {
    this.catalogService.getClusterList().subscribe(data => {
      data.forEach(element => {
        this.clusterList.push(element.clusterId);
      });
    }, error => {
      this.clusterList = [];
    });
  }

  loadStorageSystems() {
    this.dbLoading = true;
    this.catalogService.getStorageSystems()
      .subscribe(data => {
        data.forEach(element => {
          this.storageSystems.push(element);
        });
      }, error => {
        this.storageSystems = [];
        this.dbLoading = false;
      }, () => {
        this.storageSystems = this.storageSystems.sort((a, b): number => {
          return a.storageSystemName > b.storageSystemName ? 1 : -1;
        });
        this.dbLoading = false;
      });
  }

  onSubmit() {
    const submitValue = Object.assign({}, this.createForm.value);
    const objectSchema: ObjectSchemaMap = this.populateObject(submitValue);
    this.catalogService.getUserByName(objectSchema.createdUser)
      .subscribe(userOutput => {
        this.catalogService.insertObject(objectSchema)
          .subscribe(objectOutput => {
            if (this.wantToRegister) {
              this.user = userOutput;
              this.userId = this.user.userId;
              this.userName = this.user.userName;
              this.systemName = this.storageSystems.filter(storageSystem => storageSystem.storageSystemId === objectOutput.storageSystemId)[0].storageSystemName;
              const dataset = this.populateDataset(objectOutput);
              this.catalogService.insertDataset(dataset).subscribe(datasetOutput => {
                this.dialogRef.close({
                  status: 'success', objectId: objectOutput.objectId, wantToRegister: this.wantToRegister, datasetId: datasetOutput,
                });
              }, error => {
                if (error.status === 500) {
                  this.dialogRef.close({status: 'fail', error: ''});
                } else {
                  this.dialogRef.close({status: 'fail', error: error});
                }
              });
            } else {
              this.dialogRef.close({
                status: 'success', objectId: objectOutput.objectId, wantToRegister: this.wantToRegister,
              });
            }
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

  populateDataset(result: any) {
    const datasetName = this.systemName + '.' + result.containerName + '.' + result.objectName;
    const dataset: Dataset = new Dataset(datasetName, 0, this.systemName, true, result.objectId, datasetName, '', this.userName, this.userName, '', 'N', [], [], 'Y', 'Y', '');
    dataset.createdUser = this.userName;
    dataset.updatedUser = this.userName;
    dataset.storageDataSetName = datasetName;
    dataset.storageDataSetDescription = datasetName + ' created by ' + this.userName;
    dataset.storageSystemId = this.storageSystem.storageSystemId;
    dataset.storageContainerName = result.containerName;
    dataset.clusters = this.clusterList;
    dataset.objectName = result.objectName;
    dataset.isAutoRegistered = 'N';
    dataset.userId = this.userId;
    return dataset;
  }

  private populateObject(submitValue) {
    const objectSchema: ObjectSchemaMap = new ObjectSchemaMap(0, submitValue.objectName, submitValue.containerName, this.systemId, submitValue.createdUser);
    const objectAttributes = [];
    objectSchema.clusters = this.clusterList;
    objectSchema.objectSchema = [];
    objectSchema.isSelfDiscovered = 'Y';
    this.typeAttributes.forEach(attr => {
      const systemAttr = {
        objectAttributeValue: attr.storageTypeAttributeValue, storageDsAttributeKeyId: attr.storageDsAttributeKeyId,
      };
      objectAttributes.push(systemAttr);
    });
    objectSchema.objectAttributes = objectAttributes;
    return objectSchema;
  }

  onStorageSystemChange() {
    this.storageSystem = Object.assign({}, this.createForm.value).storageSystem;
    this.systemId = this.storageSystem.storageSystemId;
    this.typeAttributes = [];
    this.catalogService.getTypeAttributesAtSystemLevel(this.storageSystem.storageTypeId.toString(), 'N')
      .subscribe(data => {
        data.forEach(element => {
          this.typeAttributes.push(element);
        });
        this.typeAttributes = [...this.typeAttributes];
      }, error => {
        this.typeAttributes = [];
        this.snackbar.open('Invalid Storage Type', 'Dismiss', this.config.snackBarConfig);
      });
  }

  captureRegistrationRequest() {
    const registrationStatus = Object.assign({}, this.createForm.value).wantToRegister;
    switch (registrationStatus) {
      case 'Yes': {
        this.wantToRegister = true;
        break;
      }
      case 'No': {
        this.wantToRegister = false;
        break;
      }
      default: {
        this.wantToRegister = true;
        break;
      }
    }
  }

  updateValue(event, cell, rowIndex) {
    this.editing[rowIndex + '-' + cell] = false;
    this.typeAttributes[rowIndex][cell] = event.target.value;
    this.typeAttributes = [...this.typeAttributes];
  }
}
