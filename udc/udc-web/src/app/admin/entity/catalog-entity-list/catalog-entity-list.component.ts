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

import {Component, Input, Output, EventEmitter, ViewChild, OnInit} from '@angular/core';
import {
  MatDialog, MatDialogConfig, MatDialogRef, MatSnackBar
} from '@angular/material';
import {CatalogService} from '../../../udc/catalog/services/catalog.service';
import {ConfigService} from '../../../core/services/config.service';
import {CatalogCreateEntityDialogComponent} from '../catalog-entity-create/catalog-entity-create-dialog.component';
import {environment} from '../../../../environments/environment';
import {CloseSideNavAction} from '../../../core/store/sidenav/sidenav.actions';
import {Store} from '@ngrx/store';
import * as fromRoot from '../../../core/store';
import {SessionService} from '../../../core/services/session.service';

@Component({
  selector: 'app-catalog-entity-list', templateUrl: './catalog-entity-list.component.html',
})
export class CatalogEntityListComponent implements OnInit {
  public loading = false;
  public displayList = [];
  private entityList = [];
  public systemName = '';
  public entityId = '';
  public inProgress = false;
  public actionMsg: string;
  public admin: boolean;
  dialogConfig: MatDialogConfig = {width: '600px'};
  @ViewChild('catalogEntitesTable') table: any;

  @Input() project: string;

  @Output() loaded: EventEmitter<boolean> = new EventEmitter();
  @Output() refresh: EventEmitter<string> = new EventEmitter();

  constructor(private sessionService: SessionService, private store: Store<fromRoot.State>, private catalogService: CatalogService, private snackbar: MatSnackBar, private config: ConfigService, private dialog: MatDialog) {
    this.store.dispatch(new CloseSideNavAction());
    this.config.getAdminEmitter().subscribe(data => {
      this.admin = data;
    });
    this.admin = this.config.admin;
  }

  ngOnInit() {
    this.loadEntitiesFromCatalog();
  }

  loadEntitiesFromCatalog() {
    this.loading = true;
    this.entityList = [];
    this.catalogService.getEntityList().subscribe(data => {
      data.map(element => {
        this.entityList.push(element);
      });
    }, error => {
      this.entityList = [];
    }, () => {
      this.displayList = this.entityList.sort((a, b): number => {
        return a.entityName > b.entityName ? 1 : -1;
      });
      this.loading = false;
      this.loaded.emit(true);
    });
  }

  search(searchText: string) {
    this.displayList = this.entityList.filter((item, index, array) => {
      return item['entityName'].toLowerCase().search(searchText.toLowerCase()) !== -1 || item['entityDescription'].toLowerCase().search(searchText.toLowerCase()) !== -1;
    });
  }

  refreshRow(event: string) {
    this.loadEntitiesFromCatalog();
  }

  private finishAction(result: boolean, refresh: boolean, message: string) {
    if (refresh) {
      this.refresh.emit(this.entityId.toString());
      this.loadEntitiesFromCatalog();
    }
    this.inProgress = false;
    this.actionMsg = '';
  }

  createEntityDialog() {
    let dialogRef: MatDialogRef<CatalogCreateEntityDialogComponent>;
    dialogRef = this.dialog.open(CatalogCreateEntityDialogComponent, this.dialogConfig);
    dialogRef.afterClosed()
      .subscribe(result => {
        if (result) {
          if (result.status === 'success') {
            this.snackbar.open('Created the entity with ID -> ' + result.entityId, 'Dismiss', this.config.snackBarConfig);
            this.finishAction(true, true, 'Entities');
          } else if (result.status === 'fail') {
            const description = result.error.errorDescription || 'Unknown Error';
            this.snackbar.open(description + '.Failed to create entity', 'Dismiss', this.config.snackBarConfig);
          } else if (result.status === 'user fail') {
            const description = 'Invalid Username';
            this.snackbar.open(description + '.Failed to create entity', 'Dismiss', this.config.snackBarConfig);
          }
        }
      });
  }
}
