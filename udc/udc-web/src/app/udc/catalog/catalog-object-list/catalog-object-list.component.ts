import {
  Component, Input, Output, OnChanges, EventEmitter, ViewChild, SimpleChanges
} from '@angular/core';
import {
  MdDialog, MdDialogConfig, MdDialogRef, MdSnackBar
} from '@angular/material';
import {ConfigService} from '../../../core/services';
import {CatalogService} from '../services/catalog.service';
import {Page} from '../models/catalog-list-page';
import {ObjectSchemaMap} from '../models/catalog-objectschema';
import {StorageSystem} from '../models/catalog-storagesystem';
import {CatalogCreateObjectDialogComponent} from '../catalog-object-create/catalog-object-create-dialog.component';

@Component({
  selector: 'app-catalog-object-list', templateUrl: './catalog-object-list.component.html',
})
export class CatalogObjectListComponent implements OnChanges {
  public loading = false;
  public displayList = new Array<ObjectSchemaMap>();
  private objectsList = new Array<ObjectSchemaMap>();
  public containerName = '';
  public systemName = '';
  public clusterId = '';
  page = new Page();
  public actionMsg: string;
  public inProgress = false;
  dialogConfig: MdDialogConfig = {width: '1000px', height: '90vh'};
  @ViewChild('catalogDatasetsTable') table: any;

  @Input() project: string;
  @Input() projectType: string;
  @Input() refresh: boolean;
  @Input() systemMap: Map<number, StorageSystem>;
  @Output() loaded: EventEmitter<boolean> = new EventEmitter();
  @Output() refresh1: EventEmitter<string> = new EventEmitter();

  constructor(private catalogService: CatalogService, private snackbar: MdSnackBar, private config: ConfigService, private dialog: MdDialog) {
    this.page.pageNumber = 0;
    this.page.size = 20;
  }

  ngOnChanges(changes: SimpleChanges) {
    if (changes.projectType && changes.projectType.currentValue) {
      this.systemName = changes.projectType.currentValue;
    }
    if (changes.project && changes.project.currentValue) {
      this.containerName = changes.project.currentValue;
    }
    if (this.containerName && this.containerName !== '' && this.systemName && this.systemName !== '') {
      this.loadObjectsFromCatalog({offset: 0});
    }
    if (changes.refresh && !changes.refresh.firstChange) {
      if (this.containerName && this.containerName !== '') {
        this.loadObjectsFromCatalog({offset: 0});
      }
    }
  }

  loadObjectsFromCatalog(pageInfo) {
    this.loading = true;
    this.objectsList = [];
    this.page.pageNumber = pageInfo.offset;
    this.catalogService.getObjectListPageable(this.systemName, this.containerName, this.page).subscribe(pagedData => {
      this.objectsList = pagedData.data;
      this.page = pagedData.page;
      this.objectsList.forEach(object => object.storageSystemName = this.systemMap.get(object.storageSystemId).storageSystemName);
      this.displayList = this.objectsList;
    }, error => {
      this.objectsList = [];
      this.displayList = [];
    }, () => {
      this.loading = false;
      this.loaded.emit(true);
    });
  }

  createObjectDialog() {
    let dialogRef: MdDialogRef<CatalogCreateObjectDialogComponent>;
    dialogRef = this.dialog.open(CatalogCreateObjectDialogComponent, this.dialogConfig);
    dialogRef.afterClosed()
      .subscribe(result => {
        if (result) {
          if (result.status === 'success') {
            if (result.wantToRegister) {
              this.snackbar.open('Created the Object with ID -> ' + result.objectId + ' and Dataset with ID -> ' + result.datasetId, 'Dismiss', this.config.snackBarConfig);
            } else {
              this.snackbar.open('Created the Object with ID -> ' + result.objectId, 'Dismiss', this.config.snackBarConfig);
            }

            this.finishAction(true, true, 'Objects');
          } else if (result.status === 'fail') {
            const description = result.error.errorDescription || 'Unknown Error';
            this.snackbar.open(description + '.Failed to create Object', 'Dismiss', this.config.snackBarConfig);
          } else if (result.status === 'user fail') {
            const description = 'Invalid Username';
            this.snackbar.open(description + '.Failed to create Object', 'Dismiss', this.config.snackBarConfig);
          }
        }
      });
  }

  private finishAction(result: boolean, refresh: boolean, message: string) {
    if (refresh) {
      this.refresh1.emit(this.systemName);
      this.loadObjectsFromCatalog({offset: 0});
    }
    this.inProgress = false;
    this.actionMsg = '';
  }

  search(searchText: string) {
    this.displayList = this.objectsList.filter((item, index, array) => {
      return item['objectName'].toLowerCase().search(searchText.toLowerCase()) !== -1 || item['containerName'].toLowerCase().search(searchText.toLowerCase()) !== -1 || item['storageSystemName'].toLowerCase().search(searchText.toLowerCase()) !== -1;
    });

  }

  toggleExpandRow(row) {
    this.table.rowDetail.toggleExpandRow(row);
  }

  refreshRow(event: string) {
    this.loadObjectsFromCatalog({offset: 0});
  }
}
