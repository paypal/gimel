import {Component, Input, Output, EventEmitter} from '@angular/core';
import {Observable} from 'rxjs/Observable';
import {MdSnackBar, MdSnackBarConfig} from '@angular/material';
import {
  MdDialog, MdDialogRef, MdDialogConfig,
} from '@angular/material';

import {ConfigService} from '../../../core/services/config.service';
import {CatalogCategoryEditDialogComponent} from '../catalog-category-edit-dialog/catalog-category-edit-dialog.component';
import {CatalogService} from '../../../udc/catalog/services/catalog.service';

@Component({
  selector: 'app-catalog-category-action',
  templateUrl: './catalog-category-action.component.html',
  styleUrls: ['./catalog-category-action.component.scss'],
})

export class CatalogCategoryActionComponent {
  @Input() storageId: number;
  @Input() storageName: string;
  @Input() storageDescription: string;
  @Input() createdUser: string;
  @Input() isActiveYN: string;
  @Input() public errorStatus: boolean;
  public inProgress = false;
  public actionMsg: string;
  dialogConfig: MdDialogConfig = {width: '600px'};

  @Output() refresh: EventEmitter<string> = new EventEmitter();

  constructor(private catalogService: CatalogService, private snackbar: MdSnackBar, private config: ConfigService, private dialog: MdDialog) {
  }

  private finishAction(result: boolean, refresh: boolean, message: string) {
    if (refresh) {
      this.refresh.emit(this.storageId.toString());
    }
    this.inProgress = false;
    this.actionMsg = '';
  }

  openEditCategoryDialog() {
    let dialogRef: MdDialogRef<CatalogCategoryEditDialogComponent>;
    dialogRef = this.dialog.open(CatalogCategoryEditDialogComponent, this.dialogConfig);
    dialogRef.componentInstance.storageName = this.storageName;
    dialogRef.componentInstance.storageId = this.storageId;
    dialogRef.componentInstance.storageDescription = this.storageDescription;
    dialogRef.componentInstance.createdUser = this.createdUser;
    dialogRef.afterClosed()
      .subscribe(result => {
        if (result) {
          if (result.status === 'success') {
            this.snackbar.open('Updated the Category with ID -> ' + this.storageId, 'Dismiss', this.config.snackBarConfig);
            this.finishAction(true, true, 'Category');
          } else if (result.status === 'fail') {
            const description = result.error.errorDescription || 'Unknown Error';
            this.snackbar.open(description + '.Failed to update Category', 'Dismiss', this.config.snackBarConfig);
          } else if (result.status === 'user fail') {
            const description = 'Invalid Username';
            this.snackbar.open(description + '.Failed to update Category', 'Dismiss', this.config.snackBarConfig);
          }
        }
      });
  }

  deleteCategory() {
    this.actionMsg = 'Deactivating Category';
    this.inProgress = true;
    this.catalogService.deleteCategory(this.storageId.toString())
      .subscribe(data => {
        this.snackbar.open('Deactivated the Category with ID -> ' + this.storageId, 'Dismiss', this.config.snackBarConfig);
        this.finishAction(true, true, 'Category');
      }, error => {
        this.snackbar.open('Category ID not found -> ' + this.storageId, 'Dismiss', this.config.snackBarConfig);
        this.actionMsg = 'Category Cannot be De-Activated';
        this.inProgress = false;
      });
  }

  enableCategory() {
    this.actionMsg = 'Activating Category';
    this.inProgress = true;
    this.catalogService.enableCategory(this.storageId.toString())
      .subscribe(data => {
        this.snackbar.open('Activated the Category with ID -> ' + this.storageId, 'Dismiss', this.config.snackBarConfig);
        this.finishAction(true, true, 'Category');
      }, error => {
        this.snackbar.open('Category ID not found -> ' + this.storageId, 'Dismiss', this.config.snackBarConfig);
        this.actionMsg = 'Category Cannot be Re-Activated';
        this.inProgress = false;
      });
  }
}
