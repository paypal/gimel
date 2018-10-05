import {Component, Input, Output, EventEmitter, ViewChild, OnInit} from '@angular/core';
import {
  MdDialog, MdDialogConfig, MdDialogRef, MdSnackBar, MdSnackBarConfig
} from '@angular/material';
import {CatalogService} from '../../../udc/catalog/services/catalog.service';
import {ConfigService} from '../../../core/services/config.service';
import {CatalogCreateCategoryDialogComponent} from '../catalog-category-create/catalog-category-create-dialog.component';
import {ApiService} from '../../../core/services/api.service';

@Component({
  selector: 'app-catalog-category-list', templateUrl: './catalog-category-list.component.html',
})
export class CatalogCategoryListComponent implements OnInit {
  public loading = false;
  public displayList = [];
  private categoryList = [];
  public systemName = '';
  public storageId = '';
  public inProgress = false;
  public actionMsg: string;
  dialogConfig: MdDialogConfig = {width: '600px'};
  @ViewChild('catalogClustersTable') table: any;

  @Input() project: string;

  @Output() loaded: EventEmitter<boolean> = new EventEmitter();
  @Output() refresh: EventEmitter<string> = new EventEmitter();

  constructor(private catalogService: CatalogService, private api: ApiService, private snackbar: MdSnackBar, private config: ConfigService, private dialog: MdDialog) {

  }

  ngOnInit() {
    this.loadCategoriesFromCatalog();
  }

  loadCategoriesFromCatalog() {
    this.loading = true;
    this.categoryList = [];
    this.catalogService.getStorageCategories().subscribe(data => {
      data.map(element => {
        this.categoryList.push(element);
      });
    }, error => {
      this.categoryList = [];
    }, () => {
      this.displayList = this.categoryList.sort((a, b): number => {
        return a.storageId > b.storageId ? 1 : -1;
      });
      this.loading = false;
      this.loaded.emit(true);
    });
  }

  search(searchText: string) {
    this.displayList = this.categoryList.filter((item) => {
      return item['storageName'].toLowerCase().search(searchText.toLowerCase()) !== -1 || item['storageDescription'].toLowerCase().search(searchText.toLowerCase()) !== -1 || item['createdUser'].toLowerCase().search(searchText.toLowerCase()) !== -1;
    });
  }

  refreshRow(event: string) {
    this.loadCategoriesFromCatalog();
  }

  private finishAction(result: boolean, refresh: boolean, message: string) {
    if (refresh) {
      this.refresh.emit(this.storageId.toString());
      this.loadCategoriesFromCatalog();
    }
    this.inProgress = false;
    this.actionMsg = '';
  }

  createCategoryDialog() {
    let dialogRef: MdDialogRef<CatalogCreateCategoryDialogComponent>;
    dialogRef = this.dialog.open(CatalogCreateCategoryDialogComponent, this.dialogConfig);
    dialogRef.afterClosed()
      .subscribe(result => {
        if (result) {
          if (result.status === 'success') {
            this.snackbar.open('Created the category with ID -> ' + result.categoryId, 'Dismiss', this.config.snackBarConfig);
            this.finishAction(true, true, 'Categories');
          } else if (result.status === 'fail') {
            const description = result.error.errorDescription || 'Unknown Error';
            this.snackbar.open(description + '.Failed to create category', 'Dismiss', this.config.snackBarConfig);
          } else if (result.status === 'user fail') {
            const description = 'Invalid Username';
            this.snackbar.open(description + '.Failed to create category', 'Dismiss', this.config.snackBarConfig);
          }
        }
      });
  }
}
