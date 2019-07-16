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

import { NgModule } from '@angular/core';
import { RouterModule } from '@angular/router';
import { FlexLayoutModule } from '@angular/flex-layout';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import { MatListModule } from '@angular/material/list';
import { MatMenuModule } from '@angular/material/menu';
import { MatCardModule } from '@angular/material/card';
import { MatToolbarModule } from '@angular/material/toolbar';
import { MatSidenavModule } from '@angular/material/sidenav';
import { MatSnackBarModule } from '@angular/material/snack-bar';
import { MatDialogModule } from '@angular/material/dialog';
import { SharedModule } from '../shared/shared.module';
import { GhtoolbarComponent } from './ghtoolbar/ghtoolbar.component';
import { GhcontexthelpComponent } from './ghcontexthelp/ghcontexthelp.component';
import { SidenavContentComponent } from './sidenav-content/sidenav-content.component';
import { GlobalHeaderComponent } from './global-header.component';
import { GhcontexthelpItemComponent } from './ghcontexthelp/ghcontexthelp-item/ghcontexthelp-item.component';
import { GhsidebarComponent } from './ghsidebar/ghsidebar.component';
import { GhSettingsComponent } from './ghsettings/ghsettings.component';
import { GhSettingsDialogComponent } from './ghsettings/ghsettings-dialog/ghsettings-dialog.component';
import { MatBadgeModule, MatTooltipModule} from '@angular/material';



@NgModule({
  imports: [
    SharedModule,
    RouterModule,
    FlexLayoutModule,
    MatIconModule,
    MatListModule,
    MatMenuModule,
    MatToolbarModule,
    MatCardModule,
    MatSidenavModule,
    MatSnackBarModule,
    MatDialogModule,
    MatButtonModule,
    MatTooltipModule,
    MatBadgeModule,
  ],
  declarations: [
    GhtoolbarComponent,
    GhcontexthelpComponent,
    GhcontexthelpItemComponent,
    SidenavContentComponent,
    GlobalHeaderComponent,
    GhsidebarComponent,
    GhSettingsComponent,
    GhSettingsDialogComponent,
  ],
  exports: [
    GlobalHeaderComponent,
  ],
  entryComponents: [
    GhSettingsDialogComponent,
  ],
})
export class GlobalHeaderModule { }
