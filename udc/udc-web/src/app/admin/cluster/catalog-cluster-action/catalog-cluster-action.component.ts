import {Component, Input, Output, EventEmitter} from '@angular/core';
import {Observable} from 'rxjs/Observable';
import {MdSnackBar, MdSnackBarConfig} from '@angular/material';
import {
  MdDialog, MdDialogRef, MdDialogConfig,
} from '@angular/material';

import {ConfigService} from '../../../core/services/config.service';
import {CatalogClusterEditDialogComponent} from '../catalog-cluster-edit-dialog/catalog-cluster-edit-dialog.component';
import {CatalogService} from '../../../udc/catalog/services/catalog.service';

@Component({
  selector: 'app-catalog-cluster-action',
  templateUrl: './catalog-cluster-action.component.html',
  styleUrls: ['./catalog-cluster-action.component.scss'],
})

export class CatalogClusterActionComponent {
  @Input() clusterId: number;
  @Input() clusterName: string;
  @Input() clusterDescription: string;
  @Input() createdUser: string;
  @Input() isActiveYN: string;
  @Input() livyPort: number;
  @Input() livyEndPoint: string;
  @Input() public errorStatus: boolean;
  public inProgress = false;
  public actionMsg: string;
  dialogConfig: MdDialogConfig = {width: '600px'};

  @Output() refresh: EventEmitter<string> = new EventEmitter();

  constructor(private catalogService: CatalogService, private snackbar: MdSnackBar, private config: ConfigService, private dialog: MdDialog) {
  }

  private finishAction(result: boolean, refresh: boolean, message: string) {
    if (refresh) {
      this.refresh.emit(this.clusterId.toString());
    }
    this.inProgress = false;
    this.actionMsg = '';
  }

  openEditClusterDialog() {
    let dialogRef: MdDialogRef<CatalogClusterEditDialogComponent>;
    dialogRef = this.dialog.open(CatalogClusterEditDialogComponent, this.dialogConfig);
    dialogRef.componentInstance.clusterName = this.clusterName;
    dialogRef.componentInstance.clusterId = this.clusterId;
    dialogRef.componentInstance.clusterDescription = this.clusterDescription;
    dialogRef.componentInstance.createdUser = this.createdUser;
    dialogRef.componentInstance.livyPort = this.livyPort;
    dialogRef.componentInstance.livyEndPoint = this.livyEndPoint;
    dialogRef.afterClosed()
      .subscribe(result => {
        if (result) {
          if (result.status === 'success') {
            this.snackbar.open('Updated the Cluster with ID -> ' + this.clusterId, 'Dismiss', this.config.snackBarConfig);
            this.finishAction(true, true, 'Cluster');
          } else if (result.status === 'fail') {
            const description = result.error.errorDescription || 'Unknown Error';
            this.snackbar.open(description + '.Failed to update dataset', 'Dismiss', this.config.snackBarConfig);
          } else if (result.status === 'user fail') {
            const description = 'Invalid Username';
            this.snackbar.open(description + '.Failed to update dataset', 'Dismiss', this.config.snackBarConfig);
          }
        }
      });
  }

  deleteCluster() {
    this.actionMsg = 'Deleting Cluster';
    this.inProgress = true;
    this.catalogService.deleteCluster(this.clusterId.toString())
      .subscribe(data => {
        this.snackbar.open('Deactivated the Cluster with ID -> ' + this.clusterId, 'Dismiss', this.config.snackBarConfig);
        this.finishAction(true, true, 'Dataset');
      }, error => {
        this.snackbar.open('Cluster ID not found -> ' + this.clusterId, 'Dismiss', this.config.snackBarConfig);
        this.actionMsg = 'Cluster Cannot be De-Activated';
        this.inProgress = false;
      });
  }

  enableCluster() {
    this.actionMsg = 'Activating Cluster';
    this.inProgress = true;
    this.catalogService.enableCluster(this.clusterId.toString())
      .subscribe(data => {
        this.snackbar.open('Activated the Cluster with ID -> ' + this.clusterId, 'Dismiss', this.config.snackBarConfig);
        this.finishAction(true, true, 'Cluster');
      }, error => {
        this.snackbar.open('Cluster ID not found -> ' + this.clusterId, 'Dismiss', this.config.snackBarConfig);
        this.actionMsg = 'Cluster Cannot be Re-Activated';
        this.inProgress = false;
      });
  }
}
