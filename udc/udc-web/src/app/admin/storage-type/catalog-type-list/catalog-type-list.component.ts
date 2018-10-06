import {Component, Input, Output, OnChanges, EventEmitter, ViewChild} from '@angular/core';
import {
  MdDialog, MdDialogConfig, MdDialogRef, MdSnackBar, MdSnackBarConfig
} from '@angular/material';

import {ConfigService} from '../../../core/services';
import {CatalogService} from '../../../udc/catalog/services/catalog.service';
import {CatalogCreateTypeDialogComponent} from '../catalog-type-create/catalog-type-create-dialog.component';
import {ApiService} from '../../../core/services/api.service';

@Component({
  selector: 'app-catalog-type-list', templateUrl: './catalog-type-list.component.html',
})
export class CatalogTypeListComponent implements OnChanges {
  public loading = false;
  public displayList = [];
  private categoriesList = [];
  public storageName = '';
  public clusterId = '';
  public storageTypeId = '';
  public inProgress = false;
  public actionMsg: string;
  @ViewChild('catalogTypesTable') table: any;

  @Input() project: string;
  @Input() refresh: boolean;
  dialogConfig: MdDialogConfig = {width: '1000px', height: '90vh'};

  @Output() loaded: EventEmitter<boolean> = new EventEmitter();
  @Output() refresh1: EventEmitter<string> = new EventEmitter();

  constructor(private catalogService: CatalogService, private api: ApiService, private snackbar: MdSnackBar, private config: ConfigService, private dialog: MdDialog) {
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
    this.loading = true;
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
      this.loading = false;
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
    let dialogRef: MdDialogRef<CatalogCreateTypeDialogComponent>;
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
