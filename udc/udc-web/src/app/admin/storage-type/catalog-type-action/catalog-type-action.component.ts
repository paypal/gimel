import {Component, Input, Output, EventEmitter} from '@angular/core';
import {MdSnackBar} from '@angular/material';
import {MdDialog, MdDialogRef, MdDialogConfig} from '@angular/material';
import {forkJoin} from 'rxjs/observable/forkJoin';
import {ConfigService} from '../../../core/services/config.service';
import {CatalogService} from '../../../udc/catalog/services/catalog.service';
import {CatalogTypeEditDialogComponent} from '../catalog-type-edit-dialog/catalog-type-edit-dialog.component';
import {CatalogTypeViewAttributesDialogComponent} from '../catalog-type-view-attributes-dialog/catalog-type-view-attributes-dialog.component';

@Component({
  selector: 'app-catalog-type-action',
  templateUrl: './catalog-type-action.component.html',
  styleUrls: ['./catalog-type-action.component.scss'],
})

export class CatalogTypeActionComponent {
  @Input() storageTypeId: number;
  @Input() storageTypeName: string;
  @Input() storageTypeDescription: string;
  @Input() createdUser: string;
  @Input() isActiveYN: string;
  @Input() public errorStatus: boolean;
  public inProgress = false;
  public actionMsg: string;
  typeAttributes: Array<any>;

  dialogEditConfig: MdDialogConfig = {width: '1000px', height: '90vh'};
  dialogViewConfig: MdDialogConfig = {width: '1000px', height: '60vh'};

  @Output() refresh1: EventEmitter<string> = new EventEmitter();

  constructor(private catalogService: CatalogService, private snackbar: MdSnackBar, private config: ConfigService, private dialog: MdDialog) {
  }

  private finishAction(result: boolean, refresh: boolean, message: string) {
    if (refresh) {
      this.refresh1.emit(this.storageTypeId.toString());
    }
    this.inProgress = false;
    this.actionMsg = '';
  }

  openAttributesDialog() {
    const typeAttributes = this.catalogService.getTypeAttributes(this.storageTypeId.toString());
    forkJoin([typeAttributes]).subscribe(results => {
      this.typeAttributes = results[0];
      let dialogRef: MdDialogRef<CatalogTypeViewAttributesDialogComponent>;
      dialogRef = this.dialog.open(CatalogTypeViewAttributesDialogComponent, this.dialogViewConfig);
      dialogRef.componentInstance.storageTypeId = this.storageTypeId;
      dialogRef.componentInstance.typeAttributes = this.typeAttributes;
      dialogRef.componentInstance.storageTypeName = this.storageTypeName;
      dialogRef.afterClosed()
        .subscribe(result => {
          if (result) {
            if (result.status === 'success') {
              this.snackbar.open('Updated the Type with ID -> ' + this.storageTypeId, 'Dismiss', this.config.snackBarConfig);
              this.finishAction(true, true, 'Type');
            } else if (result.status === 'fail') {
              const description = result.error.errorDescription || 'Unknown Error';
              this.snackbar.open(description + '.Failed to update Type', 'Dismiss', this.config.snackBarConfig);
            } else if (result.status === 'user fail') {
              const description = 'Invalid Username';
              this.snackbar.open(description + '.Failed to update Type', 'Dismiss', this.config.snackBarConfig);
            }
          }
        });
    });
  }

  openEditTypeDialog() {
    const typeAttributes = this.catalogService.getTypeAttributes(this.storageTypeId.toString());
    forkJoin([typeAttributes]).subscribe(results => {
      this.typeAttributes = results[0];
      let dialogRef: MdDialogRef<CatalogTypeEditDialogComponent>;
      dialogRef = this.dialog.open(CatalogTypeEditDialogComponent, this.dialogEditConfig);
      dialogRef.componentInstance.storageTypeName = this.storageTypeName;
      dialogRef.componentInstance.storageTypeId = this.storageTypeId;
      dialogRef.componentInstance.storageTypeDescription = this.storageTypeDescription;
      dialogRef.componentInstance.createdUser = this.createdUser;
      dialogRef.componentInstance.typeAttributes = this.typeAttributes;
      dialogRef.afterClosed()
        .subscribe(result => {
          if (result) {
            if (result.status === 'success') {
              this.snackbar.open('Updated the Type with ID -> ' + this.storageTypeId, 'Dismiss', this.config.snackBarConfig);
              this.finishAction(true, true, 'Type');
            } else if (result.status === 'fail') {
              const description = result.error.errorDescription || 'Unknown Error';
              this.snackbar.open(description + '.Failed to update Type', 'Dismiss', this.config.snackBarConfig);
            } else if (result.status === 'user fail') {
              const description = 'Invalid Username';
              this.snackbar.open(description + '.Failed to update Type', 'Dismiss', this.config.snackBarConfig);
            }
          }
        });
    });
  }

  deleteType() {
    this.actionMsg = 'Deactivating Type';
    this.inProgress = true;
    this.catalogService.deleteType(this.storageTypeId.toString())
      .subscribe(data => {
        this.snackbar.open('Deactivated the Type with ID -> ' + this.storageTypeId, 'Dismiss', this.config.snackBarConfig);
        this.finishAction(true, true, 'Type');
      }, error => {
        this.snackbar.open('Type ID not found -> ' + this.storageTypeId, 'Dismiss', this.config.snackBarConfig);
        this.actionMsg = 'Type Cannot be De-Activated';
        this.inProgress = false;
      });
  }

  enableType() {
    this.actionMsg = 'Activating Type';
    this.inProgress = true;
    this.catalogService.enableType(this.storageTypeId.toString())
      .subscribe(data => {
        this.snackbar.open('Activated the Type with ID -> ' + this.storageTypeId, 'Dismiss', this.config.snackBarConfig);
        this.finishAction(true, true, 'Type');
      }, error => {
        this.snackbar.open('Type ID not found -> ' + this.storageTypeId, 'Dismiss', this.config.snackBarConfig);
        this.actionMsg = 'Type Cannot be Re-Activated';
        this.inProgress = false;
      });
  }
}
