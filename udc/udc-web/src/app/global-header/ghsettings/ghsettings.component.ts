/*
 * Copyright 2019 PayPal Inc.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { Component, Input, OnDestroy } from '@angular/core';
import {
  MatDialog, MatDialogRef, MatDialogConfig,
} from '@angular/material';
import { Store } from '@ngrx/store';

import { GhSettingsDialogComponent } from './ghsettings-dialog/ghsettings-dialog.component';
import * as fromRoot from '../../core/store';

@Component({
  selector: 'app-ghsettings',
  templateUrl: './ghsettings.component.html',
  styleUrls: ['./ghsettings.component.scss'],
})
export class GhSettingsComponent implements OnDestroy {
  globalHeaderTheme: string;
  dialogConfig: MatDialogConfig = {width: '500px'};
  subscription: any;
  @Input() admin: boolean;

  constructor(private dialog: MatDialog, private store: Store<fromRoot.State>) {
    this.subscription = this.store.select(fromRoot.getGlobalHeaderTheme).subscribe((theme) => {
      this.globalHeaderTheme = theme;
    });
  }

  ngOnDestroy() {
    this.subscription.unsubscribe();
  }

  openSettingsDialog() {
    let dialogRef: MatDialogRef<GhSettingsDialogComponent>;
    dialogRef = this.dialog.open(GhSettingsDialogComponent, this.dialogConfig);
    dialogRef.componentInstance.selectedTheme = this.globalHeaderTheme;
    dialogRef.componentInstance.isAdmin = this.admin;
    dialogRef.afterClosed().subscribe(result => {
    });
  }
}
