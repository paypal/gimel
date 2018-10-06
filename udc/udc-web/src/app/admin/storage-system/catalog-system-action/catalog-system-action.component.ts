import {Component, Input, Output, EventEmitter} from '@angular/core';
import {MdSnackBar} from '@angular/material';
import {MdDialog, MdDialogRef, MdDialogConfig} from '@angular/material';
import {forkJoin} from 'rxjs/observable/forkJoin';
import {ConfigService} from '../../../core/services/config.service';
import {CatalogService} from '../../../udc/catalog/services/catalog.service';
import {CatalogSystemViewAttributesDialogComponent} from '../catalog-system-view-attributes-dialog/catalog-system-view-attributes-dialog.component';
import {CatalogSystemEditDialogComponent} from '../catalog-system-edit-dialog/catalog-system-edit-dialog.component';

@Component({
  selector: 'app-catalog-system-action',
  templateUrl: './catalog-system-action.component.html',
  styleUrls: ['./catalog-system-action.component.scss'],
})

export class CatalogSystemActionComponent {
  @Input() storageSystemId: number;
  @Input() storageSystemName: string;
  @Input() storageSystemDescription: string;
  @Input() createdUser: string;
  @Input() isActiveYN: string;
  @Input() containers: string;
  @Input() adminUserId: number;
  @Input() isGimelCompatible: string;
  @Input() public errorStatus: boolean;
  public inProgress = false;
  public actionMsg: string;
  systemAttributes: Array<any>;

  dialogEditConfig: MdDialogConfig = {width: '600px', height: '90vh'};
  dialogViewConfig: MdDialogConfig = {width: '800px', height: '60vh'};

  @Output() refresh1: EventEmitter<string> = new EventEmitter();

  constructor(private catalogService: CatalogService, private snackbar: MdSnackBar, private config: ConfigService, private dialog: MdDialog) {
  }

  private finishAction(result: boolean, refresh: boolean, message: string) {
    if (refresh) {
      this.refresh1.emit(this.storageSystemId.toString());
    }
    this.inProgress = false;
    this.actionMsg = '';
  }

  deleteSystem() {
    this.actionMsg = 'Deactivating System';
    this.inProgress = true;
    this.catalogService.deleteSystem(this.storageSystemId.toString())
      .subscribe(data => {
        this.snackbar.open('Deactivated the Type with ID -> ' + this.storageSystemId, 'Dismiss', this.config.snackBarConfig);
        this.finishAction(true, true, 'Type');
      }, error => {
        this.snackbar.open('System ID not found -> ' + this.storageSystemId, 'Dismiss', this.config.snackBarConfig);
        this.actionMsg = 'System Cannot be De-Activated';
        this.inProgress = false;
      });
  }

  enableSystem() {
    this.actionMsg = 'Activating System';
    this.inProgress = true;
    this.catalogService.enableSystem(this.storageSystemId.toString())
      .subscribe(data => {
        this.snackbar.open('Activated the System with ID -> ' + this.storageSystemId, 'Dismiss', this.config.snackBarConfig);
        this.finishAction(true, true, 'System');
      }, error => {
        this.snackbar.open('System ID not found -> ' + this.storageSystemId, 'Dismiss', this.config.snackBarConfig);
        this.actionMsg = 'System Cannot be Re-Activated';
        this.inProgress = false;
      });
  }

  openAttributesDialog() {
    const systemAttributes = this.catalogService.getSystemAttributes(this.storageSystemName);
    forkJoin([systemAttributes]).subscribe(results => {
      this.systemAttributes = results[0];
      let dialogRef: MdDialogRef<CatalogSystemViewAttributesDialogComponent>;
      dialogRef = this.dialog.open(CatalogSystemViewAttributesDialogComponent, this.dialogViewConfig);
      dialogRef.componentInstance.storageSystemId = this.storageSystemId;
      dialogRef.componentInstance.systemAttributes = this.systemAttributes;
      dialogRef.componentInstance.storageSystemName = this.storageSystemName;
      dialogRef.afterClosed()
        .subscribe(result => {
          if (result) {
            if (result.status === 'success') {
              this.snackbar.open('Updated the System with ID -> ' + this.storageSystemId, 'Dismiss', this.config.snackBarConfig);
              this.finishAction(true, true, 'Type');
            } else if (result.status === 'fail') {
              const description = result.error.errorDescription || 'Unknown Error';
              this.snackbar.open(description + '.Failed to update System', 'Dismiss', this.config.snackBarConfig);
            } else if (result.status === 'user fail') {
              const description = 'Invalid Username';
              this.snackbar.open(description + '.Failed to update System', 'Dismiss', this.config.snackBarConfig);
            }
          }
        });
    });
  }

  openEditSystemDialog() {
    const systemAttributes =  this.catalogService.getSystemAttributes(this.storageSystemName);
    forkJoin([systemAttributes]).subscribe(results => {
      this.systemAttributes = results[0];
      let dialogRef: MdDialogRef<CatalogSystemEditDialogComponent>;
      dialogRef = this.dialog.open(CatalogSystemEditDialogComponent, this.dialogEditConfig);
      dialogRef.componentInstance.storageSystemName = this.storageSystemName;
      dialogRef.componentInstance.storageSystemId = this.storageSystemId;
      dialogRef.componentInstance.storageSystemDescription = this.storageSystemDescription;
      dialogRef.componentInstance.createdUser = this.createdUser;
      dialogRef.componentInstance.systemAttributes = this.systemAttributes;
      dialogRef.componentInstance.containers = this.containers;
      dialogRef.componentInstance.adminUserId = this.adminUserId;
      dialogRef.afterClosed()
        .subscribe(result => {
          if (result) {
            if (result.status === 'success') {
              this.snackbar.open('Updated the System with ID -> ' + this.storageSystemId, 'Dismiss', this.config.snackBarConfig);
              this.finishAction(true, true, 'Type');
            } else if (result.status === 'fail') {
              const description = result.error.errorDescription || 'Unknown Error';
              this.snackbar.open(description + '.Failed to update System', 'Dismiss', this.config.snackBarConfig);
            } else if (result.status === 'user fail') {
              const description = 'Invalid Username';
              this.snackbar.open(description + '.Failed to update System', 'Dismiss', this.config.snackBarConfig);
            }
          }
        });
    });

  }
}
