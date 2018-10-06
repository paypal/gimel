import {Component, Input, Output, EventEmitter} from '@angular/core';
import {forkJoin} from 'rxjs/observable/forkJoin';
import {MdSnackBar} from '@angular/material';

import {
  MdDialog, MdDialogRef, MdDialogConfig,
} from '@angular/material';

import {CatalogService} from '../../services/catalog.service';
import {ConfigService} from '../../../../core/services';
import {CatalogDatasetPendingEditDialogComponent} from '../catalog-dataset-pending-edit-dialog/catalog-dataset-pending-edit-dialog.component';
import {Observable} from 'rxjs/Observable';
import {CatalogObjectEditDialogComponent} from '../../catalog-dataset-list/catalog-object-edit-dialog/catalog-object-edit-dialog.component';
import {ObjectSchemaMap} from '../../models/catalog-objectschema';

@Component({
  selector: 'app-catalog-dataset-pending-action',
  templateUrl: './catalog-dataset-pending-action.component.html',
  styleUrls: ['./catalog-dataset-pending-action.component.scss'],
})

export class CatalogDatabaseActionPendingComponent {
  @Input() project: string;
  @Input() storageDataSetId: number;
  @Input() storageDataSetName: string;
  @Input() storageDataSetAliasName: string;
  @Input() storageDataSetDescription: string;
  @Input() createdUser: string;
  @Input() objectSchemaMapId: number;
  @Input() storageSystemName: string;
  @Input() storageDatasetStatus: string;
  @Input() isAutoRegistered: string;
  objectAttributes: Array<any>;
  systemAttributes: Array<any>;
  @Input() public errorStatus: boolean;
  public inProgress = false;
  public actionMsg: string;
  object: ObjectSchemaMap;
  dialogConfig: MdDialogConfig = {width: '1000px', height: '80vh'};

  @Output() refresh: EventEmitter<string> = new EventEmitter();

  constructor(private catalogService: CatalogService, private snackbar: MdSnackBar, private config: ConfigService, private dialog: MdDialog) {
  }

  private finishAction(result: boolean, refresh: boolean, message: string) {
    if (refresh) {
      this.refresh.emit(this.storageDataSetId.toString());
    }
    this.inProgress = false;
    this.actionMsg = '';
  }

  openEditDatasetDialog() {
    let dialogRef: MdDialogRef<CatalogDatasetPendingEditDialogComponent>;
    const schemaDetails: Observable<any> = this.catalogService.getSchemaDetails(this.objectSchemaMapId.toString());
    const systemDetails = this.catalogService.getSystemAttributes(this.storageSystemName);

    forkJoin([schemaDetails, systemDetails]).subscribe(results => {
      const schemaDetails = results[0] as any;
      // this.objectAttributes = results[0].objectAttributes;
      this.objectAttributes = schemaDetails.objectAttributes;
      this.systemAttributes = results[1];
      dialogRef = this.dialog.open(CatalogDatasetPendingEditDialogComponent, this.dialogConfig);
      dialogRef.componentInstance.storageDataSetName = this.storageDataSetName;
      dialogRef.componentInstance.storageDataSetId = this.storageDataSetId;
      dialogRef.componentInstance.projectName = this.project;
      dialogRef.componentInstance.storageDataSetAliasName = this.storageDataSetAliasName;
      dialogRef.componentInstance.storageDataSetDescription = this.storageDataSetDescription;
      dialogRef.componentInstance.createdUser = this.createdUser;
      dialogRef.componentInstance.objectAttributes = this.objectAttributes;
      dialogRef.componentInstance.systemAttributes = this.systemAttributes;
      dialogRef.componentInstance.storageDatasetStatus = this.storageDatasetStatus;
      dialogRef.componentInstance.objectSchemaMapId = this.objectSchemaMapId;
      dialogRef.componentInstance.storageSystemName = this.storageSystemName;
      dialogRef.componentInstance.isAutoRegistered = this.isAutoRegistered;
      dialogRef.afterClosed()
        .subscribe(result => {
          if (result) {
            if (result.status === 'success') {
              this.snackbar.open('Updated the Dataset with ID -> ' + this.storageDataSetId, 'Dismiss', this.config.snackBarConfig);
              this.finishAction(true, true, 'Dataset');
            } else if (result.status === 'fail') {
              const description = result.error.errorDescription || 'Unknown Error';
              this.snackbar.open(description + '.Failed to update dataset', 'Dismiss', this.config.snackBarConfig);
            } else if (result.status === 'user fail') {
              const description = 'Invalid Username';
              this.snackbar.open(description + '.Failed to update dataset', 'Dismiss', this.config.snackBarConfig);
            }
          }
        });
    });
  }

  openEditObjectDialog() {

    this.inProgress = true;
    const objectDetails = this.catalogService.getObjectDetails(this.objectSchemaMapId.toString());

    forkJoin([objectDetails]).subscribe(results => {
      this.object = results[0];
      this.inProgress = false;
      let dialogRef: MdDialogRef<CatalogObjectEditDialogComponent>;
      dialogRef = this.dialog.open(CatalogObjectEditDialogComponent, this.dialogConfig);
      dialogRef.componentInstance.objectName = this.object.objectName;
      dialogRef.componentInstance.objectId = this.objectSchemaMapId;
      dialogRef.componentInstance.containerName = this.object.containerName;
      dialogRef.componentInstance.storageSystemId = this.object.storageSystemId;
      dialogRef.componentInstance.objectSchema = this.object.objectSchema;
      dialogRef.componentInstance.createdUser = this.object.createdUser;
      dialogRef.componentInstance.objectAttributes = this.object.objectAttributes;
      dialogRef.afterClosed()
        .subscribe(result => {
          if (result) {
            if (result.status === 'success') {
              this.snackbar.open('Updated the Object with ID -> ' + result.objectId, 'Dismiss', this.config.snackBarConfig);
              this.finishAction(true, true, 'Object');
            } else if (result.status === 'fail') {
              const description = result.error.errorDescription || 'Unknown Error';
              this.snackbar.open(description + '.Failed to update Object', 'Dismiss', this.config.snackBarConfig);
            } else if (result.status === 'user fail') {
              const description = 'Invalid Username';
              this.snackbar.open(description + '.Failed to update Object', 'Dismiss', this.config.snackBarConfig);
            }
          }
        });
    });
  }

  deleteDataset() {
    this.actionMsg = 'Deleting dataset';
    this.inProgress = true;
    this.catalogService.deleteDataset(this.storageDataSetId.toString())
      .subscribe(data => {
        this.snackbar.open('Deleted the Dataset with ID -> ' + this.storageDataSetId, 'Dismiss', this.config.snackBarConfig);
        this.finishAction(true, true, 'Dataset');
      }, error => {
        this.snackbar.open('Dataset ID not found -> ' + this.storageDataSetId, 'Dismiss', this.config.snackBarConfig);
        this.actionMsg = 'Dataset Deleted';
        this.inProgress = false;
      });
  }
}
