import { NgModule } from '@angular/core';
import { RouterModule } from '@angular/router';
import { MaterialModule } from '@angular/material';
import { FlexLayoutModule } from '@angular/flex-layout';

import { SharedModule } from '../shared/shared.module';
import { GhtoolbarComponent } from './ghtoolbar/ghtoolbar.component';
import { GhcontexthelpComponent } from './ghcontexthelp/ghcontexthelp.component';
import { SidenavContentComponent } from './sidenav-content/sidenav-content.component';
import { GlobalHeaderComponent } from './global-header.component';
import { GhcontexthelpItemComponent } from './ghcontexthelp/ghcontexthelp-item/ghcontexthelp-item.component';
import { GhsidebarComponent } from './ghsidebar/ghsidebar.component';
import { GhSettingsComponent } from './ghsettings/ghsettings.component';
import { GhSettingsDialogComponent } from './ghsettings/ghsettings-dialog/ghsettings-dialog.component';

@NgModule({
  imports: [
    SharedModule,
    MaterialModule,
    RouterModule,
    FlexLayoutModule,
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
