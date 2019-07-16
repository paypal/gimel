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

import { Component, Input, Output, EventEmitter, ViewChild, OnInit, OnChanges } from '@angular/core';
import {
  MatDialog, MatDialogConfig, MatDialogRef, MatPaginator, MatSnackBar
} from '@angular/material';
import { CatalogService } from '../../../udc/catalog/services/catalog.service';
import { ConfigService } from '../../../core/services/config.service';
import { CloseSideNavAction } from '../../../core/store/sidenav/sidenav.actions';
import { Store} from '@ngrx/store';
import * as fromRoot from '../../../core/store';
import { SessionService } from '../../../core/services/session.service';
import { Page } from '../../../udc/catalog/models/catalog-list-page';
import { NgxSpinnerService } from 'ngx-spinner';
import { CatalogCreateClassificationDialogComponent } from '../catalog-classification-create/catalog-classification-create-dialog.component';
import { environment } from '../../../../environments/environment';

@Component({
  selector: 'app-catalog-classification-list', templateUrl: './catalog-classification-list.component.html',
})
export class CatalogClassificationListComponent implements OnInit, OnChanges {
  public loading = false;
  public displayList = [];
  private classificationList = [];
  public systemName = '';
  public entityId = '';
  public inProgress = false;
  public actionMsg: string;
  public admin: boolean;
  public classificationId;
  page = new Page();
  dialogConfig: MatDialogConfig = {width: '1000px', height: '630px'};
  @ViewChild('catalogClassificationTable') table: any;
  @Input() project: string;
  @Output() loaded: EventEmitter<boolean> = new EventEmitter();
  @Output() refresh: EventEmitter<string> = new EventEmitter();
  @ViewChild(MatPaginator) paginator: MatPaginator;

  constructor(private sessionService: SessionService, private spinner: NgxSpinnerService, private store: Store<fromRoot.State>, private catalogService: CatalogService, private snackbar: MatSnackBar, private config: ConfigService, private dialog: MatDialog) {
    this.store.dispatch(new CloseSideNavAction());
    this.page.pageNumber = 0;
    this.page.size = 10;
    this.config.getAdminEmitter().subscribe(data => {
      this.admin = data;
    });
    this.admin = this.config.admin;
  }

  ngOnInit() {
    this.loadAllClassificationsFromCatalog({offset: 0});
  }

  ngOnChanges(changes) {
    if (changes.project && changes.project.currentValue) {
      if (changes.project.currentValue.toString() !== 'All') {
        this.classificationId = changes.project.currentValue.toString();
        this.loadClassificationsFromCatalogByClassID({offset: changes.offset !== undefined ? changes.offset : 0});
      } else {
        this.classificationId = undefined;
        this.loadAllClassificationsFromCatalog({offset: changes.offset !== undefined ? changes.offset : 0});
      }
    }
  }

  ngOnPageChange(changes) {
    if (this.classificationId !== undefined) {
      this.loadClassificationsFromCatalogByClassID(changes);
    } else {
      this.loadAllClassificationsFromCatalog(changes);
    }
  }

  loadAllClassificationsFromCatalog(pageInfo) {
    this.classificationList = [];
    this.spinner.show();
    this.page.pageNumber = pageInfo.offset;
    this.catalogService.getClassificationListPageable(this.page).subscribe(pagedData => {
      this.classificationList = pagedData.data;
      this.page = pagedData.page;
      this.displayList = this.classificationList.sort((a, b): number => {
        return a.entityName > b.entityName ? 1 : -1;
      });
    }, error => {
      this.classificationList = [];
      this.displayList = [];
    }, () => {
      setTimeout(() => {
        this.spinner.hide();
      }, 0);
      this.loaded.emit(true);
    });
  }

  loadClassificationsFromCatalogByClassID(pageInfo) {
    this.classificationList = [];
    this.spinner.show();
    this.page.pageNumber = pageInfo.offset;
    this.catalogService.getClassificationListPageableByClassID(this.classificationId, this.page).subscribe(pagedData => {
      this.classificationList = pagedData.data;
      this.page = pagedData.page;
      this.displayList = this.classificationList.sort((a, b): number => {
        return a.entityName > b.entityName ? 1 : -1;
      });
    }, error => {
      this.classificationList = [];
      this.displayList = [];
    }, () => {
      setTimeout(() => {
        this.spinner.hide();
      }, 0);
      this.loaded.emit(true);
    });
  }

  search(searchText: string) {
    this.displayList = this.classificationList.filter((item, index, array) => {
      return item['objectName'].toLowerCase().search(searchText.toLowerCase()) !== -1 || item['columnName'].toLowerCase().search(searchText.toLowerCase()) !== -1;
    });
  }

  refreshRow(event: string) {
    this.loadAllClassificationsFromCatalog({offset: 0});
  }

  private finishAction(result: boolean, refresh: boolean, message: string) {
    if (refresh) {
      this.refresh.emit(this.entityId.toString());
      this.loadAllClassificationsFromCatalog({offset: 0});
    }
    this.inProgress = false;
    this.actionMsg = '';
  }

  createClassificationDialog() {
    let dialogRef: MatDialogRef<CatalogCreateClassificationDialogComponent>;
    dialogRef = this.dialog.open(CatalogCreateClassificationDialogComponent, this.dialogConfig);
    dialogRef.afterClosed()
      .subscribe(result => {
        if (result) {
          if (result.status === 'success') {
            this.snackbar.open('Created the classification with ID -> ' + result.datasetClassificationId, 'Dismiss', this.config.snackBarConfig);
            this.finishAction(true, true, 'Entities');
          } else if (result.status === 'fail') {
            const description = result.error.errorDescription || 'Unknown Error';
            this.snackbar.open(description + '.Failed to create classification', 'Dismiss', this.config.snackBarConfig);
          } else if (result.status === 'user fail') {
            const description = 'Invalid Username';
            this.snackbar.open(description + '.Failed to create classification', 'Dismiss', this.config.snackBarConfig);
          }
        }
      });
  }

  redirectToBulkUpload() {
    window.location.href = environment.protocol + environment.devHost + ':' + environment.uiport + '/udc/#/udc/upload';
  }
}
