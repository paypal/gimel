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

import {Component, Input, Output, OnChanges, EventEmitter, ViewChild, SimpleChanges, Inject} from '@angular/core';
import {ConfigService} from '../../../core/services';
import {CatalogService} from '../services/catalog.service';
import {Page} from '../models/catalog-list-page';
import {Dataset} from '../models/catalog-dataset';
import {CloseSideNavAction} from '../../../core/store/sidenav/sidenav.actions';
import {Store} from '@ngrx/store';
import * as fromRoot from '../../../core/store';
import {NgxSpinnerService} from 'ngx-spinner';
import {SessionService} from '../../../core/services/session.service';
import {MatDialogRef, MatPaginator} from '@angular/material';
import {MatDialog, MatDialogConfig} from '@angular/material';
import {CatalogDatasetDetailDialogComponent} from './catalog-dataset-detail-dialog/catalog-dataset-detail-dialog.component';
import {DatasetDescriptionDialogComponent} from '../../../dataset-description-dialog/dataset-description-dialog.component';

@Component({
  selector: 'app-catalog-dataset-list-registered', templateUrl: './catalog-dataset-list.component.html', styleUrls: ['./catalog-dataset-list.component.scss']
})

export class CatalogDatabaseListComponent implements OnChanges {
  public datasetDisplayList = new Array<Dataset>();
  private datasetList = [];
  public systemName = '';
  public allSystems = '';
  public containerName = '';
  public typeName = '';
  public zoneName = '';
  public clusterId = '';
  public copiedDataset = '';
  // public searchString = '';
  public admin: boolean;
  public userName = '';
  public entityName = '';
  page = new Page();
  public storageDataSetName = '';
  public length = 0;
  public pageSize = 10;
  public name = '';
  @ViewChild(MatPaginator) paginator: MatPaginator;
  @ViewChild('catalogDatasetsTable') table: any;
  dialogViewConfig: MatDialogConfig = {width: '1000px', height: '90vh'};

  @Input() searchString: string;

  @Input() projectContainer: string;
  @Input() project: string;
  @Input() projectType: string;
  @Input() projectZone: string;

  @Input() allProjects: string;
  @Input() searchType: string;

  @Output() loaded: EventEmitter<boolean> = new EventEmitter();

  @Output() datasetSize: EventEmitter<number> = new EventEmitter();
  @Output() datasetsBySchemaSize: EventEmitter<number> = new EventEmitter();
  @Output() datasetsByAttributesSize: EventEmitter<number> = new EventEmitter();
  @Output() datasetsByTagSize: EventEmitter<number> = new EventEmitter();
  @Output() datasetsByDescriptionSize: EventEmitter<number> = new EventEmitter();

  constructor(private spinner: NgxSpinnerService, private store: Store<fromRoot.State>, private catalogService: CatalogService, private config: ConfigService, private dialog: MatDialog, private sessionService: SessionService) {
    this.store.dispatch(new CloseSideNavAction());
    this.page.pageNumber = 0;
    this.page.size = this.pageSize;

    this.config.getAdminEmitter().subscribe(data => {
      this.admin = data;
    });
    this.admin = this.config.admin;

    this.config.getUserNameEmitter().subscribe(data => {
      this.userName = data;
    });
    this.userName = this.config.userName;
  }

  ngOnChanges(changes: SimpleChanges) {
    if (changes.projectType && changes.projectType.currentValue) {
      this.typeName = changes.projectType.currentValue;
    }
    if (changes.projectZone && changes.projectZone.currentValue) {
      this.zoneName = changes.projectZone.currentValue;
    }
    if (changes.project && changes.project.currentValue) {
      this.systemName = changes.project.currentValue;
    }
    if (changes.allProjects && changes.allProjects.currentValue) {
      this.allSystems = changes.allProjects.currentValue;
    }
    if (changes.projectContainer && changes.projectContainer.currentValue) {
      this.containerName = changes.projectContainer.currentValue;
    }
    if (this.systemName && this.systemName !== '' && this.typeName && this.typeName !== '') {
      this.loadDatasetsFromCatalog({pageIndex: 0});
    }
  }

  loadDatasetsFromCatalog(pageInfo) {
    this.spinner.show();
    this.datasetList = [];
    this.page.pageNumber = pageInfo.pageIndex;
    if (pageInfo.pageSize) {
      this.page.size = pageInfo.pageSize;
    }

    this.catalogService.getDataSetListPageable(this.searchString.toLowerCase(), this.containerName, this.allSystems, this.typeName, this.page, this.searchType).subscribe(pagedData => {
      this.datasetList = pagedData.data;
      this.length = pagedData.page.totalElements;
      if (this.searchType === 'DATASET') {
        this.datasetSize.emit(this.length);
      } else if (this.searchType === 'OBJECT_SCHEMA') {
        this.datasetsBySchemaSize.emit(this.length);
      } else if (this.searchType === 'OBJECT_ATTRIBUTE') {
        this.datasetsByAttributesSize.emit(this.length);
      } else if (this.searchType === 'TAG') {
        this.datasetsByTagSize.emit(this.length);
      } else if (this.searchType === 'DATASET_DESCRIPTION') {
        this.datasetsByDescriptionSize.emit(this.length);
      }
      this.datasetList = pagedData.data;
      this.page = pagedData.page;
      this.datasetDisplayList = this.datasetList;
    }, error => {
      this.datasetList = [];
      this.datasetDisplayList = [];
    }, () => {
      setTimeout(() => {
        this.spinner.hide();
      }, 0);
      this.loaded.emit(true);

    });
  }

  toggleExpandRow(row) {
    this.table.rowDetail.toggleExpandRow(row);
  }

  refreshRow(event: string) {
    this.loadDatasetsFromCatalog({pageIndex: 0});
  }

  removeCopyText() {
    this.copiedDataset = '';
  }

  copyText(val: string, type: string) {
    if (type == 'copy') {
      this.copiedDataset = val;
    }
    let selBox = document.createElement('textarea');
    selBox.style.position = 'fixed';
    selBox.style.left = '0';
    selBox.style.top = '0';
    selBox.style.opacity = '0';
    selBox.value = val;
    document.body.appendChild(selBox);
    selBox.focus();
    selBox.select();
    document.execCommand('copy');
    document.body.removeChild(selBox);
  }

  copyUrl(val: string) {
    const currentUrl = window.location.href;
    this.copiedDataset = val;
    const strippedUrl = currentUrl.substring(0, currentUrl.indexOf('/udc/datasets') + '/udc/datasets'.length);
    const modifiedUrl = strippedUrl.replace('/udc/datasets', '/standalone/dataset/') + val;
    this.copyText(modifiedUrl, 'share');
  }

  openDialog(name, description) {
    let dialogRef: MatDialogRef<DatasetDescriptionDialogComponent>;
    dialogRef = this.dialog.open(DatasetDescriptionDialogComponent, this.dialogViewConfig);
    dialogRef.componentInstance.datasetName = name;
    dialogRef.componentInstance.datasetDescription = description;
    dialogRef.afterClosed().subscribe();
  }

  openDialogDetails(id, name) {
    let dialogRef: MatDialogRef<CatalogDatasetDetailDialogComponent>;
    dialogRef = this.dialog.open(CatalogDatasetDetailDialogComponent, this.dialogViewConfig);
    dialogRef.componentInstance.storageDataSetName = name;
    dialogRef.componentInstance.storageDataSetId = id;
    dialogRef.afterClosed().subscribe();
  }

}
