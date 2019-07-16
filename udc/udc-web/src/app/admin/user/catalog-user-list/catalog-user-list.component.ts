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

import { Component, Input, Output, EventEmitter, ViewChild, OnInit } from '@angular/core';
import { MatDialog, MatDialogConfig, MatDialogRef, MatSnackBar } from '@angular/material';
import { CatalogService } from '../../../udc/catalog/services/catalog.service';
import { ConfigService } from '../../../core/services/config.service';
import { CatalogCreateUserDialogComponent } from '../catalog-user-create/catalog-user-create-dialog.component';
import { environment } from '../../../../environments/environment';
import { CloseSideNavAction } from '../../../core/store/sidenav/sidenav.actions';
import { Store } from '@ngrx/store';
import * as fromRoot from '../../../core/store';
import { NgxSpinnerService } from 'ngx-spinner';
import {SessionService} from '../../../core/services/session.service';

@Component({
  selector: 'app-catalog-user-list', templateUrl: './catalog-user-list.component.html',
})
export class CatalogUserListComponent implements OnInit {
  public displayList = [];
  private userList = [];
  public userId = '';
  public inProgress = false;
  public actionMsg: string;
  public admin: boolean;

  dialogConfig: MatDialogConfig = {width: '600px'};
  @ViewChild('catalogClustersTable') table: any;

  @Input() project: string;

  @Output() loaded: EventEmitter<boolean> = new EventEmitter();
  @Output() refresh: EventEmitter<string> = new EventEmitter();

  constructor(private spinner: NgxSpinnerService, private store: Store<fromRoot.State>, private catalogService: CatalogService, private snackbar: MatSnackBar, private config: ConfigService, private dialog: MatDialog, private sessionService: SessionService) {
    this.store.dispatch(new CloseSideNavAction());
    this.config.getAdminEmitter().subscribe(data => {
      this.admin = data;
    });
    this.admin = this.config.admin;
  }

  ngOnInit() {
    this.loadUsersFromCatalog();
  }

  loadUsersFromCatalog() {
    this.spinner.show();
    this.userList = [];
    this.catalogService.getUsersList().subscribe(data => {
      data.map(element => {
        this.userList.push(element);
      });
    }, error => {
      this.userList = [];
    }, () => {
      this.displayList = this.userList.sort((a, b): number => {
        return a.userName > b.userName ? 1 : -1;
      });
      setTimeout(() => {
        this.spinner.hide();
      }, 0);
      this.loaded.emit(true);
    });
  }

  search(searchText: string) {
    this.displayList = this.userList.filter((item, index, array) => {
      return item['userName'].toLowerCase().search(searchText.toLowerCase()) !== -1 || item['userFullName'].toLowerCase().search(searchText.toLowerCase()) !== -1;
    });
  }

  refreshRow(event: string) {
    this.loadUsersFromCatalog();
  }

  private finishAction(result: boolean, refresh: boolean, message: string) {
    if (refresh) {
      this.refresh.emit(this.userId.toString());
      this.loadUsersFromCatalog();
    }
    this.inProgress = false;
    this.actionMsg = '';
  }

  createUserDialag() {
    let dialogRef: MatDialogRef<CatalogCreateUserDialogComponent>;
    dialogRef = this.dialog.open(CatalogCreateUserDialogComponent, this.dialogConfig);
    dialogRef.afterClosed()
      .subscribe(result => {
        if (result) {
          if (result.status === 'success') {
            this.snackbar.open('Created the user with ID -> ' + result.userId, 'Dismiss', this.config.snackBarConfig);
            this.finishAction(true, true, 'Users');
          } else if (result.status === 'fail') {
            const description = result.error.errorDescription || 'Unknown Error';
            this.snackbar.open(description + '.Failed to create user', 'Dismiss', this.config.snackBarConfig);
          } else if (result.status === 'user fail') {
            const description = 'Invalid Username';
            this.snackbar.open(description + '.Failed to create user', 'Dismiss', this.config.snackBarConfig);
          }
        }
      });
  }
}
