import {Component, Input, Output, EventEmitter} from '@angular/core';
import {Observable} from 'rxjs/Observable';
import {MdSnackBar, MdSnackBarConfig} from '@angular/material';
import {
  MdDialog, MdDialogRef, MdDialogConfig,
} from '@angular/material';

import {ConfigService} from '../../../core/services/config.service';
import {CatalogZoneEditDialogComponent} from '../catalog-zone-edit-dialog/catalog-zone-edit-dialog.component';
import {CatalogService} from '../../../udc/catalog/services/catalog.service';

@Component({
  selector: 'app-catalog-zone-action',
  templateUrl: './catalog-zone-action.component.html',
  styleUrls: ['./catalog-zone-action.component.scss'],
})

export class CatalogZoneActionComponent {
  @Input() zoneId: number;
  @Input() zoneName: string;
  @Input() zoneDescription: string;
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
      this.refresh.emit(this.zoneId.toString());
    }
    this.inProgress = false;
    this.actionMsg = '';
  }

  openEditZoneDialog() {
    let dialogRef: MdDialogRef<CatalogZoneEditDialogComponent>;
    dialogRef = this.dialog.open(CatalogZoneEditDialogComponent, this.dialogConfig);
    dialogRef.componentInstance.zoneName = this.zoneName;
    dialogRef.componentInstance.zoneId = this.zoneId;
    dialogRef.componentInstance.zoneDescription = this.zoneDescription;
    dialogRef.componentInstance.createdUser = this.createdUser;
    dialogRef.afterClosed()
      .subscribe(result => {
        if (result) {
          if (result.status === 'success') {
            this.snackbar.open('Updated the zone with ID -> ' + this.zoneId, 'Dismiss', this.config.snackBarConfig);
            this.finishAction(true, true, 'Cluster');
          } else if (result.status === 'fail') {
            const description = result.error.errorDescription || 'Unknown Error';
            this.snackbar.open(description + '.Failed to update zone', 'Dismiss', this.config.snackBarConfig);
          } else if (result.status === 'user fail') {
            const description = 'Invalid Username';
            this.snackbar.open(description + '.Failed to update zone', 'Dismiss', this.config.snackBarConfig);
          }
        }
      });
  }

  deleteZone() {
    this.actionMsg = 'Deleting zone';
    this.inProgress = true;
    this.catalogService.deleteZone(this.zoneId.toString())
      .subscribe(data => {
        this.snackbar.open('Deactivated the Zone with ID -> ' + this.zoneId, 'Dismiss', this.config.snackBarConfig);
        this.finishAction(true, true, 'Dataset');
      }, error => {
        this.snackbar.open('Zone ID not found -> ' + this.zoneId, 'Dismiss', this.config.snackBarConfig);
        this.actionMsg = 'Zone Cannot be De-Activated';
        this.inProgress = false;
      });
  }

  enableZone() {
    this.actionMsg = 'Activating Cluster';
    this.inProgress = true;
    this.catalogService.enableZone(this.zoneId.toString())
      .subscribe(data => {
        this.snackbar.open('Activated the Zone with ID -> ' + this.zoneId, 'Dismiss', this.config.snackBarConfig);
        this.finishAction(true, true, 'Zone');
      }, error => {
        this.snackbar.open('Zone ID not found -> ' + this.zoneId, 'Dismiss', this.config.snackBarConfig);
        this.actionMsg = 'Zone Cannot be Re-Activated';
        this.inProgress = false;
      });
  }
}
