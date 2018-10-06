import {Component, Input, Output, EventEmitter, ViewChild, OnInit} from '@angular/core';
import {
  MdDialog, MdDialogConfig, MdDialogRef, MdSnackBar, MdSnackBarConfig
} from '@angular/material';
import {CatalogService} from '../../../udc/catalog/services/catalog.service';
import {ConfigService} from '../../../core/services/config.service';
import {CatalogCreateClusterDialogComponent} from '../catalog-cluster-create/catalog-cluster-create-dialog.component';

@Component({
  selector: 'app-catalog-cluster-list', templateUrl: './catalog-cluster-list.component.html',
})
export class CatalogClusterListComponent implements OnInit {
  public loading = false;
  public displayList = [];
  private datasetList = [];
  public systemName = '';
  public clusterId = '';
  public inProgress = false;
  public actionMsg: string;

  dialogConfig: MdDialogConfig = {width: '600px'};
  @ViewChild('catalogClustersTable') table: any;

  @Input() project: string;

  @Output() loaded: EventEmitter<boolean> = new EventEmitter();
  @Output() refresh: EventEmitter<string> = new EventEmitter();

  constructor(private catalogService: CatalogService, private snackbar: MdSnackBar, private config: ConfigService, private dialog: MdDialog) {
  }

  ngOnInit() {
    this.loadClustersFromCatalog();
  }

  loadClustersFromCatalog() {
    this.loading = true;
    this.datasetList = [];
    this.catalogService.getClusterList().subscribe(data => {
      data.map(element => {
        this.datasetList.push(element);
      });
    }, error => {
      this.datasetList = [];
    }, () => {
      this.displayList = this.datasetList.sort((a, b): number => {
        return a.storageId > b.storageId ? 1 : -1;
      });
      this.loading = false;
      this.loaded.emit(true);
    });
  }

  search(searchText: string) {
    this.displayList = this.datasetList.filter((item, index, array) => {
      return item['clusterName'].toLowerCase().search(searchText.toLowerCase()) !== -1 || item['storageDescription'].toLowerCase().search(searchText.toLowerCase()) !== -1;
    });
  }

  refreshRow(event: string) {
    this.loadClustersFromCatalog();
  }

  private finishAction(result: boolean, refresh: boolean, message: string) {
    if (refresh) {
      this.refresh.emit(this.clusterId.toString());
      this.loadClustersFromCatalog();
    }
    this.inProgress = false;
    this.actionMsg = '';
  }

  createClusterDialog() {
    let dialogRef: MdDialogRef<CatalogCreateClusterDialogComponent>;
    dialogRef = this.dialog.open(CatalogCreateClusterDialogComponent, this.dialogConfig);
    dialogRef.afterClosed()
      .subscribe(result => {
        if (result) {
          if (result.status === 'success') {
            this.snackbar.open('Created the cluster with ID -> ' + result.clusterId, 'Dismiss', this.config.snackBarConfig);
            this.finishAction(true, true, 'Clusters');
          } else if (result.status === 'fail') {
            const description = result.error.errorDescription || 'Unknown Error';
            this.snackbar.open(description + '.Failed to create cluster', 'Dismiss', this.config.snackBarConfig);
          } else if (result.status === 'user fail') {
            const description = 'Invalid Username';
            this.snackbar.open(description + '.Failed to create cluster', 'Dismiss', this.config.snackBarConfig);
          }
        }
      });
  }
}
