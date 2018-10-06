import {Component, Input, Output, EventEmitter, ViewChild, OnInit} from '@angular/core';
import {
  MdDialog, MdDialogConfig, MdDialogRef, MdSnackBar,
  MdSnackBarConfig
} from '@angular/material';
import {CatalogService} from '../../../udc/catalog/services/catalog.service';
import {ConfigService} from '../../../core/services/config.service';
import {CatalogCreateUserDialogComponent} from '../catalog-user-create/catalog-user-create-dialog.component';

@Component({
  selector: 'app-catalog-user-list', templateUrl: './catalog-user-list.component.html',
})
export class CatalogUserListComponent implements OnInit {
  public loading = false;
  public displayList = [];
  private userList = [];
  public userId = '';
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
    this.loadUsersFromCatalog();
  }

  loadUsersFromCatalog() {
    this.loading = true;
    this.userList = [];
    this.catalogService.getUsersList().subscribe(data => {
      data.map(element => {
        this.userList.push(element);
      });
    }, error => {
      this.userList = [];
    }, () => {
      this.displayList = this.userList.sort((a, b): number => {
        return a.userId > b.userId ? 1 : -1;
      });
      this.loading = false;
      this.loaded.emit(true);
    });
  }

  search(searchText: string) {
    this.displayList = this.userList.filter((item, index, array) => {
      return item['userName'].toLowerCase().search(searchText.toLowerCase()) !== -1 || item['userFullName'].toLowerCase().search(searchText.toLowerCase()) !== -1;
    });
  }

  refreshRow(event: string) {
    this.loadUsersFromCatalog();
  }

  private finishAction(result: boolean, refresh: boolean, message: string) {
    if (refresh) {
      this.refresh.emit(this.userId.toString());
      this.loadUsersFromCatalog();
    }
    this.inProgress = false;
    this.actionMsg = '';
  }

  createUserDialag() {
    let dialogRef: MdDialogRef<CatalogCreateUserDialogComponent>;
    dialogRef = this.dialog.open(CatalogCreateUserDialogComponent, this.dialogConfig);
    dialogRef.afterClosed()
      .subscribe(result => {
        if (result) {
          if (result.status === 'success') {
            this.snackbar.open('Created the user with ID -> ' + result.userId, 'Dismiss', this.config.snackBarConfig);
            this.finishAction(true, true, 'Users');
          } else if (result.status === 'fail') {
            const description = result.error.errorDescription || 'Unknown Error';
            this.snackbar.open(description + '.Failed to create user', 'Dismiss', this.config.snackBarConfig);
          } else if (result.status === 'user fail') {
            const description = 'Invalid Username';
            this.snackbar.open(description + '.Failed to create user', 'Dismiss', this.config.snackBarConfig);
          }
        }
      });
  }
}
