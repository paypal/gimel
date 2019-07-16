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
import { MatDialog, MatSnackBar } from '@angular/material';
import { System } from '../../models/catalog-system';
import { ConfigService } from '../../../core/services/config.service';
import { environment } from '../../../../environments/environment';
import { ThemePalette } from '@angular/material';
import {SessionService} from '../../../core/services/session.service';

export interface ChipColor {
  name: string;
  color: ThemePalette;
}

@Component({
  selector: 'app-catalog-system-create-dialog',
  templateUrl: './catalog-system-create-dialog.component.html',
  styleUrls: ['./catalog-system-create-dialog.component.scss'],
})

export class CatalogCreateSystemDialogComponent implements OnInit {
  heading = 'Create Datastore';
  createForm: FormGroup;
  maxCharsForName = 100;
  maxCharsForUserName = 50;
  maxCharsForDescName = 1000;
  createdUser: string;
  public storageTypes = [];
  public userList = [];
  public clusterList = [];
  public zoneList = [];
  public entityList = [];
  public frequencyList = Array<string>();
  public compatibilityList = Array<string>();
  public typeAttributes = Array<any>();
  public dbLoading = false;
  public clusterLoading = false;
  public zoneLoading = false;
  public entityLoading = false;
  public userLoading = false;
  public selectedStorageType: string;

  rows = [];
  editing = {};
  public readonly nameHint = 'Valid characters are a-z,0-9 and -. Names should not start with -.';
  public readonly usernameHint = 'Valid characters are a-z.';
  private readonly regex = '^(([A-Za-z0-9]+\.)*[A-Za-z0-9]+)*$';
  public availableColor: ChipColor = {name: 'Primary', color: 'primary'};

  formErrors = {
    'storageSystemName': '', 'storageSystemDescription': '', 'createdUser': '', 'containers': '',
  };

  validationMessages = {
    'storageSystemName': {
      'required': 'Storage System Name is required.',
      'maxlength': `name cannot be more than ${ this.maxCharsForName } characters long.`,
      'pattern': this.nameHint,
    }, 'createdUser': {
      'required': 'username is required.',
      'maxlength': `name cannot be more than ${ this.maxCharsForUserName } characters long.`,
      'pattern': this.usernameHint,
    }, 'storageSystemDescription': {
      'required': 'Storage System description is required.',
      'maxlength': `name cannot be more than ${ this.maxCharsForDescName } characters long.`,
      'pattern': this.nameHint,
    }, 'containers': {
      'required': 'Containers are required. Default is All',
      'maxlength': `name cannot be more than ${ this.maxCharsForDescName } characters long.`,
      'pattern': this.nameHint,
    },
  };

  constructor(private config: ConfigService, public dialogRef: MatDialogRef<CatalogCreateSystemDialogComponent>, private snackbar: MatSnackBar, private dialog: MatDialog, private fb: FormBuilder, private catalogService: CatalogService, private sessionService: SessionService) {
  }

  ngOnInit() {
    this.compatibilityList.push('Y');
    this.compatibilityList.push('N');
    this.frequencyList.push('15');
    this.frequencyList.push('60');
    this.frequencyList.push('120');
    this.frequencyList.push('240');
    this.frequencyList.push('1440');
    this.loadStorageTypes();
    this.loadUsers();
    this.loadClusters();
    this.loadZones();
    this.loadEntities();
    this.createForm = this.fb.group({
      'storageSystemName': ['', [CustomValidators.required, Validators.maxLength(this.maxCharsForName), Validators.pattern(this.regex)]],
      'storageSystemDescription': ['', [CustomValidators.required, Validators.maxLength(this.maxCharsForDescName)]],
      'createdUser': ['', [CustomValidators.required, Validators.maxLength(this.maxCharsForUserName), Validators.pattern(this.regex)]],
      'storageType': [],
      'adminUser': [],
      'cluster': [],
      'zone': [],
      'entity': [],
      'runningCluster': [],
      'isReadCompatible': [],
      'discoverySla': [],
      'containers': ['', [CustomValidators.required, Validators.maxLength(this.maxCharsForDescName)]],
    });

    this.createForm.valueChanges.subscribe(data => onValueChanged(this.createForm, this.formErrors, this.validationMessages));
    onValueChanged(this.createForm, this.formErrors, this.validationMessages);
  }

  cancel() {
    this.dialogRef.close();
  }

  loadZones() {
    this.zoneLoading = true;
    this.catalogService.getZonesList().subscribe(data => {
      data.forEach(element => {
        this.zoneList.push(element);
      });
    }, error => {
      this.zoneList = [];
      this.zoneLoading = false;
    }, () => {
      this.zoneList = this.zoneList.sort((a, b): number => {
        return a.zoneName > b.zoneName ? 1 : -1;
      });
    });
    this.zoneLoading = false;

  }
  loadClusters() {
    this.clusterLoading = true;
    this.catalogService.getClusterList().subscribe(data => {
      data.forEach(element => {
        this.clusterList.push(element);
      });
    }, error => {
      this.clusterList = [];
      this.clusterLoading = false;
    }, () => {
      this.clusterList = this.clusterList.sort((a, b): number => {
        return a.clusterName > b.clusterName ? 1 : -1;
      });
    });
    this.clusterLoading = false;
  }

  loadUsers() {
    this.userLoading = true;
    this.catalogService.getUsersList().subscribe(data => {
      data.forEach(element => {
        this.userList.push(element);
      });
    }, error => {
      this.userList = [];
      this.userLoading = false;
    }, () => {
      this.userList = this.userList.sort((a, b): number => {
        return a.userName > b.userName ? 1 : -1;
      });
    });
    this.userLoading = false;
  }

  loadStorageTypes() {
    this.dbLoading = true;
    this.catalogService.getStorageTypes()
      .subscribe(data => {
        data.forEach(element => {
          this.storageTypes.push(element);

        });
      }, error => {
        this.storageTypes = [];
        this.dbLoading = false;
      }, () => {
        this.storageTypes = this.storageTypes.sort((a, b): number => {
          return a.storageTypeName > b.storageTypeName ? 1 : -1;
        });
      });
    this.dbLoading = false;
  }

  loadEntities() {
    this.entityLoading = true;
    this.catalogService.getEntityList().subscribe(data => {
      data.forEach(element => {
        this.entityList.push(element);
      });
    }, error => {
      this.entityList = [];
      this.entityLoading = false;
    }, () => {
      this.entityList = this.entityList.sort((a, b): number => {
        return a.entityName > b.entityName ? 1 : -1;
      });
    });
    this.entityLoading = false;

  }

  onSubmit() {
    const submitValue = Object.assign({}, this.createForm.value);
    const system: System = this.populateSystem(submitValue);
    this.catalogService.getUserByName(system.createdUser)
      .subscribe(data => {
        this.catalogService.insertSystem(system)
          .subscribe(result => {
            this.dialogRef.close({status: 'success', systemId: result.storageSystemId});
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

  private populateSystem(submitValue) {
    const system: System = new System();
    const systemAttributes = [];
    const dot = '.';
    system.storageSystemDescription = submitValue.storageSystemDescription;
    system.createdUser = submitValue.createdUser;
    system.storageSystemName = submitValue.storageType.storageTypeName + dot + submitValue.storageSystemName;
    system.updatedUser = submitValue.createdUser;
    system.containers = submitValue.containers;
    system.storageTypeId = submitValue.storageType.storageTypeId;
    system.adminUserId = submitValue.adminUser.userId;
    system.assignedClusterId = submitValue.cluster.clusterId;
    system.runningClusterId = submitValue.runningCluster.clusterId;
    system.zoneId = submitValue.zone.zoneId;
    system.entityId = submitValue.entity.entityId;
    system.isReadCompatible = submitValue.isReadCompatible;
    system.discoverySla = submitValue.discoverySla;
    this.typeAttributes.forEach(attr => {
      const systemAttr = {
        storageSystemAttributeValue: attr.storageTypeAttributeValue,
        storageDataSetAttributeKeyId: attr.storageDsAttributeKeyId,
      };
      systemAttributes.push(systemAttr);
    });
    system.systemAttributeValues = systemAttributes;
    return system;
  }

  onStorageTypeChange() {
    const storageType = Object.assign({}, this.createForm.value).storageType;
    this.selectedStorageType = storageType.storageTypeName;
    this.typeAttributes = [];
    this.catalogService.getTypeAttributesAtSystemLevel(storageType.storageTypeId, 'Y')
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

  updateValue(event, cell, rowIndex) {
    this.editing[rowIndex + '-' + cell] = false;
    this.typeAttributes[rowIndex][cell] = event.target.value;
    this.typeAttributes = [...this.typeAttributes];
  }
}
