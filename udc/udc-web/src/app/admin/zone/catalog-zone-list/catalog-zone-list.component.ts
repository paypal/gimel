import {Component, Input, Output, EventEmitter, ViewChild, OnInit} from '@angular/core';
import {
  MdDialog, MdDialogConfig, MdDialogRef, MdSnackBar, MdSnackBarConfig
} from '@angular/material';
import {CatalogService} from '../../../udc/catalog/services/catalog.service';
import {ConfigService} from '../../../core/services/config.service';
import {CatalogCreateZoneDialogComponent} from '../catalog-zone-create/catalog-zone-create-dialog.component';

@Component({
  selector: 'app-catalog-zone-list', templateUrl: './catalog-zone-list.component.html',
})
export class CatalogZoneListComponent implements OnInit {
  public loading = false;
  public displayList = [];
  private datasetList = [];
  public systemName = '';
  public zoneId = '';
  public inProgress = false;
  public actionMsg: string;
  dialogConfig: MdDialogConfig = {width: '600px'};
  @ViewChild('catalogZonesTable') table: any;

  @Input() project: string;

  @Output() loaded: EventEmitter<boolean> = new EventEmitter();
  @Output() refresh: EventEmitter<string> = new EventEmitter();

  constructor(private catalogService: CatalogService, private snackbar: MdSnackBar, private config: ConfigService, private dialog: MdDialog) {
  }

  ngOnInit() {
    this.loadZonesFromCatalog();
  }

  loadZonesFromCatalog() {
    this.loading = true;
    this.datasetList = [];
    this.catalogService.getZonesList().subscribe(data => {
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
      return item['zoneName'].toLowerCase().search(searchText.toLowerCase()) !== -1 || item['zoneDescription'].toLowerCase().search(searchText.toLowerCase()) !== -1;
    });
  }

  refreshRow(event: string) {
    this.loadZonesFromCatalog();
  }

  private finishAction(result: boolean, refresh: boolean, message: string) {
    if (refresh) {
      this.refresh.emit(this.zoneId.toString());
      this.loadZonesFromCatalog();
    }
    this.inProgress = false;
    this.actionMsg = '';
  }

  createZoneDialog() {
    let dialogRef: MdDialogRef<CatalogCreateZoneDialogComponent>;
    dialogRef = this.dialog.open(CatalogCreateZoneDialogComponent, this.dialogConfig);
    dialogRef.afterClosed()
      .subscribe(result => {
        if (result) {
          if (result.status === 'success') {
            this.snackbar.open('Created the zone with ID -> ' + result.zoneId, 'Dismiss', this.config.snackBarConfig);
            this.finishAction(true, true, 'Zones');
          } else if (result.status === 'fail') {
            const description = result.error.errorDescription || 'Unknown Error';
            this.snackbar.open(description + '.Failed to create zone', 'Dismiss', this.config.snackBarConfig);
          } else if (result.status === 'user fail') {
            const description = 'Invalid Username';
            this.snackbar.open(description + '.Failed to create zone', 'Dismiss', this.config.snackBarConfig);
          }
        }
      });
  }
}
