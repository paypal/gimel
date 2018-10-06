import {Component, Input, Output, EventEmitter} from '@angular/core';
import {Observable} from 'rxjs/Observable';
import {MdSnackBar, MdSnackBarConfig} from '@angular/material';
import {
  MdDialog, MdDialogRef, MdDialogConfig,
} from '@angular/material';

import {ConfigService} from '../../../core/services/config.service';
import {CatalogService} from '../../../udc/catalog/services/catalog.service';
import {CatalogUserEditDialogComponent} from '../catalog-user-edit-dialog/catalog-user-edit-dialog.component';

@Component({
  selector: 'app-catalog-user-action',
  templateUrl: './catalog-user-action.component.html',
  styleUrls: ['./catalog-user-action.component.scss'],
})

export class CatalogUserActionComponent {
  @Input() userId: number;
  @Input() userName: string;
  @Input() userFullName: string;
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
      this.refresh.emit(this.userId.toString());
    }
    this.inProgress = false;
    this.actionMsg = '';
  }

  openEditUserDialog() {
    let dialogRef: MdDialogRef<CatalogUserEditDialogComponent>;
    dialogRef = this.dialog.open(CatalogUserEditDialogComponent, this.dialogConfig);
    dialogRef.componentInstance.userName = this.userName;
    dialogRef.componentInstance.userId = this.userId;
    dialogRef.componentInstance.userFullName = this.userFullName;
    dialogRef.afterClosed()
      .subscribe(result => {
        if (result) {
          if (result.status === 'success') {
            this.snackbar.open('Updated the user with ID -> ' + this.userId, 'Dismiss', this.config.snackBarConfig);
            this.finishAction(true, true, 'User');
          } else if (result.status === 'fail') {
            const description = result.error.errorDescription || 'Unknown Error';
            this.snackbar.open(description + '.Failed to update user', 'Dismiss', this.config.snackBarConfig);
          } else if (result.status === 'user fail') {
            const description = 'Invalid Username';
            this.snackbar.open(description + '.Failed to update user', 'Dismiss', this.config.snackBarConfig);
          }
        }
      });
  }

  deleteUser() {
    this.actionMsg = 'Deleting User';
    this.inProgress = true;
    this.catalogService.deleteUser(this.userId.toString())
      .subscribe(data => {
        this.snackbar.open('Deactivated the user with ID -> ' + this.userId, 'Dismiss', this.config.snackBarConfig);
        this.finishAction(true, true, 'User');
      }, error => {
        this.snackbar.open('User ID not found -> ' + this.userId, 'Dismiss', this.config.snackBarConfig);
        this.actionMsg = 'User Cannot be De-Activated';
        this.inProgress = false;
      });
  }

  enableUser() {
    this.actionMsg = 'Activating User';
    this.inProgress = true;
    this.catalogService.enableUser(this.userId.toString())
      .subscribe(data => {
        this.snackbar.open('Activated the user with ID -> ' + this.userId, 'Dismiss', this.config.snackBarConfig);
        this.finishAction(true, true, 'User');
      }, error => {
        this.snackbar.open('User ID not found -> ' + this.userId, 'Dismiss', this.config.snackBarConfig);
        this.actionMsg = 'User Cannot be Re-Activated';
        this.inProgress = false;
      });
  }
}
