import {Component, Input, Output, EventEmitter} from '@angular/core';
import {forkJoin} from 'rxjs/observable/forkJoin';
import {MdSnackBar} from '@angular/material';
import {MdDialog, MdDialogRef, MdDialogConfig} from '@angular/material';

import {CatalogService} from '../../services/catalog.service';
import {ConfigService} from '../../../../core/services';
import {CatalogObjectEditComponent} from '../catalog-object-edit-dialog/catalog-object-edit-dialog.component';
import {ObjectSchemaMap} from '../../models/catalog-objectschema';
import {StorageSystem} from '../../models/catalog-storagesystem';

@Component({
  selector: 'app-catalog-object-action',
  templateUrl: './catalog-object-action.component.html',
  styleUrls: ['./catalog-object-action.component.scss'],
})

export class CatalogObjectActionComponent {
  @Input() project: string;
  @Input() objectId: number;
  @Input() objectName: string;
  @Input() containerName: string;
  @Input() storageSystemName: string;
  @Input() createdUser: string;
  @Input() storageSystemId: number;
  @Input() systemMap: Map<number, StorageSystem>;
  @Input() public errorStatus: boolean;
  public inProgress = false;
  public actionMsg: string;
  object: ObjectSchemaMap;
  dialogConfig: MdDialogConfig = {width: '1000px', height: '90vh'};

  @Output() refresh: EventEmitter<string> = new EventEmitter();

  constructor(private catalogService: CatalogService, private snackbar: MdSnackBar, private config: ConfigService, private dialog: MdDialog) {
  }

  private finishAction(result: boolean, refresh: boolean, message: string) {
    if (refresh) {
      this.refresh.emit(this.objectId.toString());
    }
    this.inProgress = false;
    this.actionMsg = '';
  }
  
  openEditObjectDialog() {

    this.inProgress = true;
    const objectDetails = this.catalogService.getObjectDetails(this.objectId.toString());

    forkJoin([objectDetails]).subscribe(results => {
      this.object = results[0];
      this.inProgress = false;
      let dialogRef: MdDialogRef<CatalogObjectEditComponent>;
      dialogRef = this.dialog.open(CatalogObjectEditComponent, this.dialogConfig);
      dialogRef.componentInstance.objectName = this.object.objectName;
      dialogRef.componentInstance.objectId = this.objectId;
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
}
