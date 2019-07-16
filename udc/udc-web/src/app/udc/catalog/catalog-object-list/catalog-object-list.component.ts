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

import {
  Component, Input, Output, OnChanges, EventEmitter, ViewChild, SimpleChanges
} from '@angular/core';
import {
  MatDialog, MatDialogConfig, MatDialogRef, MatSnackBar
} from '@angular/material';
import {ConfigService} from '../../../core/services';
import {CatalogService} from '../services/catalog.service';
import {Page} from '../models/catalog-list-page';
import {ObjectSchemaMap} from '../models/catalog-objectschema';
import {StorageSystem} from '../models/catalog-storagesystem';
import {CatalogCreateObjectDialogComponent} from '../catalog-object-create/catalog-object-create-dialog.component';
import {environment} from '../../../../environments/environment';
import {CloseSideNavAction} from '../../../core/store/sidenav/sidenav.actions';
import {Store} from '@ngrx/store';
import * as fromRoot from '../../../core/store';
import {NgxSpinnerService} from 'ngx-spinner';
import {SessionService} from '../../../core/services/session.service';

@Component({
  selector: 'app-catalog-object-list', templateUrl: './catalog-object-list.component.html',
})
export class CatalogObjectListComponent implements OnChanges {
  public displayList = new Array<ObjectSchemaMap>();
  private objectsList = new Array<ObjectSchemaMap>();
  public systemName = '';
  public clusterId = '';
  page = new Page();
  public actionMsg: string;
  public searchString = '';
  public admin: boolean;
  public inProgress = false;
  public objectStr = 'All';
  dialogConfig: MatDialogConfig = {width: '1000px', height: '90vh'};
  @ViewChild('catalogDatasetsTable') table: any;

  @Input() projectType: string;
  @Input() refresh: boolean;
  @Input() systemMap: Map<number, StorageSystem>;
  @Output() loaded: EventEmitter<boolean> = new EventEmitter();
  @Output() refresh1: EventEmitter<string> = new EventEmitter();

  constructor(private sessionService: SessionService, private spinner: NgxSpinnerService, private store: Store<fromRoot.State>, private catalogService: CatalogService, private snackbar: MatSnackBar, private config: ConfigService, private dialog: MatDialog) {
    this.store.dispatch(new CloseSideNavAction());
    this.page.pageNumber = 0;
    this.page.size = 20;
    this.config.getAdminEmitter().subscribe(data => {
      this.admin = data;
    });
    this.admin = this.config.admin;
  }

  ngOnChanges(changes: SimpleChanges) {
    if (changes.projectType && changes.projectType.currentValue) {
      this.systemName = changes.projectType.currentValue;
    }
    if (this.systemName && this.systemName !== '') {
      this.loadObjectsFromCatalog({offset: 0});
    }
    if (changes.refresh && !changes.refresh.firstChange) {
      this.loadObjectsFromCatalog({offset: 0});
    }
  }

  loadObjectsFromCatalog(pageInfo) {
    this.objectsList = [];
    this.spinner.show();
    this.page.pageNumber = pageInfo.offset;
    this.catalogService.getObjectListPageable(this.objectStr, this.systemName, this.page).subscribe(pagedData => {
      this.objectsList = pagedData.data;
      this.page = pagedData.page;
      this.objectsList.forEach(object => object.storageSystemName = this.systemMap.get(object.storageSystemId).storageSystemName);
      this.displayList = this.objectsList;
    }, error => {
      this.objectsList = [];
      this.displayList = [];
    }, () => {
      setTimeout(() => {
        this.spinner.hide();
      }, 0);
      this.loaded.emit(true);
    });
  }

  createObjectDialog() {
    let dialogRef: MatDialogRef<CatalogCreateObjectDialogComponent>;
    dialogRef = this.dialog.open(CatalogCreateObjectDialogComponent, this.dialogConfig);
    dialogRef.afterClosed()
      .subscribe(result => {
        if (result) {
          if (result.status === 'success') {
            if (result.wantToRegister) {
              this.snackbar.open('Created the Object with ID -> ' + result.objectId + ' and Dataset with ID -> ' + result.datasetId, 'Dismiss', this.config.snackBarConfig);
            } else {
              this.snackbar.open('Created the Object with ID -> ' + result.objectId, 'Dismiss', this.config.snackBarConfig);
            }

            this.finishAction(true, true, 'Objects');
          } else if (result.status === 'fail') {
            const description = result.error.errorDescription || 'Unknown Error';
            this.snackbar.open(description + '.Failed to create Object', 'Dismiss', this.config.snackBarConfig);
          } else if (result.status === 'user fail') {
            const description = 'Invalid Username';
            this.snackbar.open(description + '.Failed to create Object', 'Dismiss', this.config.snackBarConfig);
          }
        }
      });
  }

  private finishAction(result: boolean, refresh: boolean, message: string) {
    if (refresh) {
      this.refresh1.emit(this.systemName);
      this.loadObjectsFromCatalog({offset: 0});
    }
    this.inProgress = false;
    this.actionMsg = '';
  }

  search(searchText: string) {
    this.displayList = this.objectsList.filter((item, index, array) => {
      return item['objectName'].toLowerCase().search(searchText.toLowerCase()) !== -1 || item['containerName'].toLowerCase().search(searchText.toLowerCase()) !== -1 || item['storageSystemName'].toLowerCase().search(searchText.toLowerCase()) !== -1;
    });

  }

  toggleExpandRow(row) {
    this.table.rowDetail.toggleExpandRow(row);
  }

  refreshRow(event: string) {
    this.loadObjectsFromCatalog({offset: 0});
  }

  searchObjects() {
    this.objectStr = this.searchString;
    this.loadObjectsFromCatalog({offset: 0});
  }
}
