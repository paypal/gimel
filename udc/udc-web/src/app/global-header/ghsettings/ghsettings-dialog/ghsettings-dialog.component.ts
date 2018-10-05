import { Component } from '@angular/core';
import { MdDialogRef } from '@angular/material';

@Component({
  selector: 'app-ghsettings-dialog',
  templateUrl: './ghsettings-dialog.component.html',
  styleUrls: ['./ghsettings-dialog.component.scss'],
})
export class GhSettingsDialogComponent {

  selectedTheme = '';

  constructor(public dialogRef: MdDialogRef<GhSettingsDialogComponent>) { }

  onSubmit() {
    this.dialogRef.close({
      theme: this.selectedTheme,
    });
  }
}
