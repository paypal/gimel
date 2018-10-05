import {Component, Input, Output, OnChanges, EventEmitter, ViewChild} from '@angular/core';
import {
  MdDialog, MdDialogConfig, MdDialogRef, MdSnackBar, MdSnackBarConfig
} from '@angular/material';

import {ConfigService} from '../../../core/services';
import {CatalogService} from '../../../udc/catalog/services/catalog.service';
import {CatalogCreateSystemDialogComponent} from '../catalog-system-create/catalog-system-create-dialog.component';
import {ApiService} from '../../../core/services/api.service';

@Component({
  selector: 'app-catalog-system-list', templateUrl: './catalog-system-list.component.html',
})
export class CatalogSystemListComponent implements OnChanges {
  public loading = false;
  public displayList = [];
  private typesList = [];
  public storageTypeName = '';
  public storageTypeId = '';
  public inProgress = false;
  public actionMsg: string;
  @ViewChild('catalogSystemsTable') table: any;

  @Input() project: string;
  @Input() refresh: boolean;
  dialogConfig: MdDialogConfig = {width: '1000px', height: '90vh'};

  @Output() loaded: EventEmitter<boolean> = new EventEmitter();
  @Output() refresh1: EventEmitter<string> = new EventEmitter();

  constructor(private catalogService: CatalogService, private api: ApiService, private snackbar: MdSnackBar, private config: ConfigService, private dialog: MdDialog) {

  }

  ngOnChanges(changes) {
    if (changes.project && changes.project.currentValue) {
      this.storageTypeName = changes.project.currentValue;
      if (this.storageTypeName && this.storageTypeName !== '') {
        this.loadSystemForType();

      }
    }
    if (changes.refresh && !changes.refresh.firstChange) {
      if (this.storageTypeName && this.storageTypeName !== '') {
        this.loadSystemForType();
      }
    }
  }

  loadSystemForType() {
    this.loading = true;
    this.typesList = [];
    this.catalogService.getSystemList(this.storageTypeName).subscribe(data => {
      data.map(element => {
        this.typesList.push(element);
      });
    }, error => {
      this.typesList = [];
    }, () => {
      this.displayList = this.typesList.sort((a, b): number => {
        return a.storageSystemId > b.storageSystemId ? 1 : -1;
      });
      this.loading = false;
      this.loaded.emit(true);
    });
  }

  search(searchText: string) {
    this.displayList = this.typesList.filter((item, index, array) => {
      return item['storageSystemName'].toLowerCase().search(searchText.toLowerCase()) !== -1 || item['isActiveYN'].toLowerCase().search(searchText.toLowerCase()) !== -1;
    });
  }

  toggleExpandRow(row) {
    this.table.rowDetail.toggleExpandRow(row);
  }

  refreshRow(event: string) {
    this.loadSystemForType();
  }

  private finishAction(result: boolean, refresh: boolean, message: string) {
    if (refresh) {
      this.refresh1.emit(this.storageTypeId.toString());
      this.loadSystemForType();
    }
    this.inProgress = false;
    this.actionMsg = '';
  }

  createSystemDialog() {
    let dialogRef: MdDialogRef<CatalogCreateSystemDialogComponent>;
    dialogRef = this.dialog.open(CatalogCreateSystemDialogComponent, this.dialogConfig);
    dialogRef.afterClosed()
      .subscribe(result => {
        if (result) {
          if (result.status === 'success') {
            this.snackbar.open('Created the System with ID -> ' + result.systemId, 'Dismiss', this.config.snackBarConfig);
            this.finishAction(true, true, 'Systems');
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
}
