import {Component, Input, OnDestroy} from '@angular/core';
import {
  MdDialog, MdDialogRef, MdDialogConfig,
} from '@angular/material';
import {Store} from '@ngrx/store';

import {GhSettingsDialogComponent} from './ghsettings-dialog/ghsettings-dialog.component';
import * as fromRoot from '../../core/store';

@Component({
  selector: 'app-ghsettings',
  templateUrl: './ghsettings.component.html',
  styleUrls: ['./ghsettings.component.scss'],
})
export class GhSettingsComponent implements OnDestroy {
  globalHeaderTheme: string;
  dialogConfig: MdDialogConfig = {width: '500px'};
  subscription: any;

  constructor(private dialog: MdDialog, private store: Store<fromRoot.State>) {
    this.subscription = this.store.select(fromRoot.getGlobalHeaderTheme).subscribe((theme) => {
      this.globalHeaderTheme = theme;
    });
  }

  ngOnDestroy() {
    this.subscription.unsubscribe();
  }

  openSettingsDialog() {
    let dialogRef: MdDialogRef<GhSettingsDialogComponent>;
    dialogRef = this.dialog.open(GhSettingsDialogComponent, this.dialogConfig);
    dialogRef.componentInstance.selectedTheme = this.globalHeaderTheme;
    dialogRef.afterClosed().subscribe(result => {
    });
  }
}
