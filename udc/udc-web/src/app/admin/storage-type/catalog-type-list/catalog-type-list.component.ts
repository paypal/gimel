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

import { Component, Input, Output, OnChanges, EventEmitter, ViewChild } from '@angular/core';
import {
  MatDialog, MatDialogConfig, MatDialogRef, MatSnackBar, MatSnackBarConfig
} from '@angular/material';

import { ConfigService } from '../../../core/services';
import { CatalogService } from '../../../udc/catalog/services/catalog.service';
import { CatalogCreateTypeDialogComponent } from '../catalog-type-create/catalog-type-create-dialog.component';
import { ApiService } from '../../../core/services/api.service';
import { environment } from '../../../../environments/environment';
import { CloseSideNavAction } from '../../../core/store/sidenav/sidenav.actions';
import { Store } from '@ngrx/store';
import * as fromRoot from '../../../core/store';
import { NgxSpinnerService } from 'ngx-spinner';
import {SessionService} from '../../../core/services/session.service';


@Component({
  selector: 'app-catalog-type-list', templateUrl: './catalog-type-list.component.html',
})
export class CatalogTypeListComponent implements OnChanges {
  public displayList = [];
  private categoriesList = [];
  public storageName = '';
  public clusterId = '';
  public storageTypeId = '';
  public inProgress = false;
  public actionMsg: string;
  public admin: boolean;
  @ViewChild('catalogTypesTable') table: any;

  @Input() project: string;
  @Input() refresh: boolean;
  dialogConfig: MatDialogConfig = {width: '1000px', height: '90vh'};

  @Output() loaded: EventEmitter<boolean> = new EventEmitter();
  @Output() refresh1: EventEmitter<string> = new EventEmitter();

  constructor(private spinner: NgxSpinnerService, private store: Store<fromRoot.State>, private catalogService: CatalogService, private api: ApiService, private snackbar: MatSnackBar, private config: ConfigService, private dialog: MatDialog, private sessionService: SessionService) {
    this.store.dispatch(new CloseSideNavAction());
    this.config.getAdminEmitter().subscribe(data => {
      this.admin = data;
    });
    this.admin = this.config.admin;
  }

  ngOnChanges(changes) {
    if (changes.project && changes.project.currentValue) {
      this.storageName = changes.project.currentValue;
      if (this.storageName && this.storageName !== '') {
        this.loadTypesForCategory();

      }
    }
    if (changes.refresh && !changes.refresh.firstChange) {
      if (this.storageName && this.storageName !== '') {
        this.loadTypesForCategory();
      }
    }
  }

  loadTypesForCategory() {
    this.spinner.show();
    this.categoriesList = [];
    this.catalogService.getTypeList(this.storageName).subscribe(data => {
      data.map(element => {
        this.categoriesList.push(element);

      });
    }, error => {
      this.categoriesList = [];
    }, () => {
      this.displayList = this.categoriesList.sort((a, b): number => {
        return a.storageTypeId > b.storageTypeId ? 1 : -1;
      });
      setTimeout(() => {
        this.spinner.hide();
      }, 0);
      this.loaded.emit(true);
    });
  }

  search(searchText: string) {
    this.displayList = this.categoriesList.filter((item, index, array) => {
      return item['storageTypeName'].toLowerCase().search(searchText.toLowerCase()) !== -1 || item['isActiveYN'].toLowerCase().search(searchText.toLowerCase()) !== -1;
    });
  }

  toggleExpandRow(row) {
    this.table.rowDetail.toggleExpandRow(row);
  }

  refreshRow(event: string) {
    this.loadTypesForCategory();
  }

  private finishAction(result: boolean, refresh: boolean, message: string) {
    if (refresh) {
      this.refresh1.emit(this.storageTypeId.toString());
      this.loadTypesForCategory();
    }
    this.inProgress = false;
    this.actionMsg = '';
  }

  createTypeDialog() {
    let dialogRef: MatDialogRef<CatalogCreateTypeDialogComponent>;
    dialogRef = this.dialog.open(CatalogCreateTypeDialogComponent, this.dialogConfig);
    dialogRef.afterClosed()
      .subscribe(result => {
        if (result) {
          if (result.status === 'success') {
            this.snackbar.open('Created the Type with ID -> ' + result.typeId, 'Dismiss', this.config.snackBarConfig);
            this.finishAction(true, true, 'Types');
          } else if (result.status === 'fail') {
            const description = result.error.errorDescription || 'Unknown Error';
            this.snackbar.open(description + '.Failed to create type', 'Dismiss', this.config.snackBarConfig);
          } else if (result.status === 'user fail') {
            const description = 'Invalid Username';
            this.snackbar.open(description + '.Failed to create type', 'Dismiss', this.config.snackBarConfig);
          }
        }
      });
  }
}
