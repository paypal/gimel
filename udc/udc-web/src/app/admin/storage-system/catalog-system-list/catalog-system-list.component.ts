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
import { MatDialog, MatDialogConfig, MatDialogRef, MatSnackBar } from '@angular/material';
import { ConfigService } from '../../../core/services';
import { CatalogService } from '../../../udc/catalog/services/catalog.service';
import { CatalogCreateSystemDialogComponent } from '../catalog-system-create/catalog-system-create-dialog.component';
import { ApiService } from '../../../core/services/api.service';
import { environment } from '../../../../environments/environment';
import { CloseSideNavAction } from '../../../core/store/sidenav/sidenav.actions';
import { Store } from '@ngrx/store';
import * as fromRoot from '../../../core/store';
import { CatalogSystemDiscoveryMetricsDialogComponent } from '../catalog-system-discovery-dialog/catalog-system-discovery-metrics-dialog.component';
import { NgxSpinnerService } from 'ngx-spinner';
import {SessionService} from '../../../core/services/session.service';

@Component({
  selector: 'app-catalog-system-list', templateUrl: './catalog-system-list.component.html',
})
export class CatalogSystemListComponent implements OnChanges {
  public displayList = [];
  private systemsList = [];
  public systemForZoneList = [];
  public systemForEntityList = [];
  private discoveryList = [];
  public storageTypeName = '';
  public storageTypeId = '';
  public entityId = '';
  public zoneId = '';
  public inProgress = false;
  public actionMsg: string;
  public admin: boolean;
  public entityName = '';
  public zoneName = '';
  @ViewChild('catalogSystemsTable') table: any;

  @Input() project: string;
  @Input() entity: string;
  @Input() zone: string;
  @Input() refresh: boolean;
  @Input() erefresh: boolean;
  @Input() zrefresh: boolean;

  dialogConfig: MatDialogConfig = {width: '1000px', height: '90vh'};
  dialogViewConfig: MatDialogConfig = {width: '1000px', height: '80vh'};
  @Output() loaded: EventEmitter<boolean> = new EventEmitter();
  @Output() zloaded: EventEmitter<boolean> = new EventEmitter();
  @Output() eloaded: EventEmitter<boolean> = new EventEmitter();
  @Output() refreshSystemForType: EventEmitter<string> = new EventEmitter();
  @Output() refreshSystemForEntity: EventEmitter<string> = new EventEmitter();
  @Output() refreshSystemForZone: EventEmitter<string> = new EventEmitter();

  constructor(private sessionService: SessionService, private spinner: NgxSpinnerService, private store: Store<fromRoot.State>, private catalogService: CatalogService, private api: ApiService, private snackbar: MatSnackBar, private config: ConfigService, private dialog: MatDialog) {
    this.store.dispatch(new CloseSideNavAction());
    this.config.getAdminEmitter().subscribe(data => {
      this.admin = data;
    });
    this.admin = this.config.admin;
  }

  ngOnChanges(changes) {

    if (changes.project && changes.project.currentValue) {
      this.storageTypeName = changes.project.currentValue;
      if (this.storageTypeName && this.storageTypeName !== '') {
        this.loadSystemForType();
      }
    } else if (changes.zone && changes.zone.currentValue) {
      this.zoneName = changes.zone.currentValue;
      if (this.zoneName && this.zoneName !== '') {
        this.loadSystemForZone();
      }
    } else if (changes.entity && changes.entity.currentValue) {
      this.entityName = changes.entity.currentValue;
      if (this.entityName && this.entityName !== '') {
        this.loadSystemForEntity();
      }
    }
    if (changes.refresh && !changes.refresh.firstChange) {
      if (this.storageTypeName && this.storageTypeName !== '') {
        this.loadSystemForType();
      }
    } else if (changes.zrefresh && !changes.zrefresh.firstChange) {
      if (this.zoneName && this.zoneName !== '') {
        this.loadSystemForZone();
      }
    } else if (changes.erefresh && !changes.erefresh.firstChange) {
      if (this.entityName && this.entityName !== '') {
        this.loadSystemForEntity();
      }
    }
  }

  loadSystemForEntity() {
    this.spinner.show();
    this.discoveryList = [];
    this.systemForEntityList = [];
    this.systemsList.forEach(system => {
      if (this.entity !== 'All') {
        if (this.entity === system.entityName) {
          this.systemForEntityList.push(system);
        }
      }
    });
    if (this.entity === 'All') {
      this.displayList = this.systemsList.sort((a, b): number => {
        return a.storageSystemId > b.storageSystemId ? 1 : -1;
      });
    } else {
      this.displayList = this.systemForEntityList.sort((a, b): number => {
        return a.storageSystemId > b.storageSystemId ? 1 : -1;
      });
    }
    setTimeout(() => {
      this.spinner.hide();
    }, 1000);
    this.displayList = [...this.displayList];
    this.eloaded.emit(true);
  }

  loadSystemForZone() {
    this.spinner.show();
    this.systemForZoneList = [];
    this.systemsList.forEach(system => {
      if (this.zone !== 'All') {
          if (this.zone === system.zoneName) {
            this.systemForZoneList.push(system);
          }
        }
      });
    if (this.zone === 'All') {
        this.displayList = this.systemsList.sort((a, b): number => {
          return a.storageSystemId > b.storageSystemId ? 1 : -1;
        });
      } else {
          this.displayList = this.systemForZoneList.sort((a, b): number => {
          return a.storageSystemId > b.storageSystemId ? 1 : -1;
        });
      }
    setTimeout(() => {
      this.spinner.hide();
    }, 1000);
    this.displayList = [...this.displayList];
    this.zloaded.emit(true);
  }

  loadSystemForType() {
    this.spinner.show();
    this.systemsList = [];
    this.discoveryList = [];
    this.catalogService.getSystemList(this.storageTypeName).subscribe(data => {
      data.map(element => {
        this.systemsList.push(element);
      });
      if (this.zone !== 'All') {
      this.loadSystemForZone();
    } if (this.entity !== 'All') {
      this.loadSystemForEntity();
    }
      const systemIds = this.systemsList.map(type => type.storageSystemId).join(',');
      this.catalogService.getDiscoverySla(systemIds).subscribe(data1 => {
        this.discoveryList = data1;
        this.systemsList.forEach(system => {
          const currentDiscovery = this.discoveryList.filter(discovery => discovery.storageSystemId === system.storageSystemId)[0];
          if (currentDiscovery) {
            system.lastInvocation = currentDiscovery.startTime;
            system.totalInserts = currentDiscovery.totalInserts;
            system.totalDeletes = currentDiscovery.totalDeletes;
            system.totalUpdates = currentDiscovery.totalUpserts;
          } else {
            system.lastInvocation = '';
            system.totalInserts = 0;
            system.totalDeletes = 0;
            system.totalUpdates = 0;
          }
      });
        this.systemsList.forEach(system => {
          if (system.lastInvocation !== '') {
            const date = new Date();
            if (system.discoverySla * 2 > (date.getTime() - new Date(system.lastInvocation).getTime()) / 60000) {
              system.runStatus = 'LATEST';
            } else {
              system.runStatus = 'STALE';
            }
          } else {
            system.runStatus = 'OUTDATED';
          }
        });
      }, error => {
        this.systemsList = [];
        setTimeout(() => {
          this.spinner.hide();
        }, 0);
      });
    }, error => {
      setTimeout(() => {
        this.spinner.hide();
      }, 1000);
      this.systemsList = [];
    }, () => {
      this.displayList = this.systemsList.sort((a, b): number => {
        return a.storageSystemId > b.storageSystemId ? 1 : -1;
      });
      setTimeout(() => {
        this.spinner.hide();
      }, 0);
      this.displayList = [...this.displayList];
      this.loaded.emit(true);
    });
  }

  search(searchText: string) {
    this.displayList = this.systemsList.filter((item, index, array) => {
      return item['storageSystemName'].toLowerCase().search(searchText.toLowerCase()) !== -1 || item['isActiveYN'].toLowerCase().search(searchText.toLowerCase()) !== -1;
    });
  }

  toggleExpandRow(row) {
    this.table.rowDetail.toggleExpandRow(row);
  }

  refreshRowType(event: string) {
    this.loadSystemForType();
  }

  refreshRowEntity(event: string) {
    this.loadSystemForEntity();
  }

  refreshRowZone(event: string) {
    this.loadSystemForZone();
  }

  private finishAction(result: boolean, refresh: boolean, message: string, erefresh: boolean, zrefresh: boolean) {
    if (refresh) {
      this.refreshSystemForType.emit(this.storageTypeId.toString());
      this.loadSystemForType();
    }
    if (zrefresh) {
      this.refreshSystemForZone.emit(this.zoneId.toString());
      this.loadSystemForZone();
    }
    if (erefresh) {
      this.refreshSystemForEntity.emit(this.entityId.toString());
      this.loadSystemForEntity();
    }
    this.inProgress = false;
    this.actionMsg = '';
  }

  createSystemDialog() {
    let dialogRef: MatDialogRef<CatalogCreateSystemDialogComponent>;
    dialogRef = this.dialog.open(CatalogCreateSystemDialogComponent, this.dialogConfig);
    dialogRef.afterClosed()
      .subscribe(result => {
        if (result) {
          if (result.status === 'success') {
            this.snackbar.open('Created the System with ID -> ' + result.systemId, 'Dismiss', this.config.snackBarConfig);
            this.finishAction(true, true, 'Systems',true,true);
          } else if (result.status === 'fail') {
            const description = result.error.errorDescription || 'Unknown Error';
            this.snackbar.open(description + '.Failed to create System', 'Dismiss', this.config.snackBarConfig);
          } else if (result.status === 'user fail') {
            const description = 'Invalid Username';
            this.snackbar.open(description + '.Failed to create System', 'Dismiss', this.config.snackBarConfig);
          }
        }
      });
  }

  openDiscoveryStatistics(row) {
    this.actionMsg = 'Viewing Attributes';
    this.inProgress = true;
    let dialogRef: MatDialogRef<CatalogSystemDiscoveryMetricsDialogComponent>;
    dialogRef = this.dialog.open(CatalogSystemDiscoveryMetricsDialogComponent, this.dialogViewConfig);
    dialogRef.componentInstance.storageSystemName = row.storageSystemName;
    dialogRef.componentInstance.storageSystemId = row.storageSystemId;
    dialogRef.componentInstance.lastInvocation = row.lastInvocation;
    dialogRef.componentInstance.totalInserts = row.totalInserts;
    dialogRef.componentInstance.totalUpdates = row.totalUpdates;
    dialogRef.componentInstance.totalDeletes = row.totalDeletes;
    dialogRef.componentInstance.discoverySla = row.discoverySla;
    dialogRef.componentInstance.runStatus = row.runStatus;
    dialogRef.afterClosed()
      .subscribe(result => {
        this.inProgress = false;
      });

  }
}
