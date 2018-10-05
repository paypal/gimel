import { CommonModule } from '@angular/common';
import { FlexLayoutModule } from '@angular/flex-layout';
import { MaterialModule } from '@angular/material';
import { NgModule } from '@angular/core';
import { NgxDatatableModule } from '@swimlane/ngx-datatable';
import { ReactiveFormsModule } from '@angular/forms';

import { UdcHomeComponent } from './udc-home.component';
import { UdcHomeRoutingModule } from './udc-home.routing.module';
import { SharedModule } from '../shared/shared.module';
import {HomeSearchComponent} from './search/udc-home-search.component';
import {GhSearchboxResultComponent} from './search/gh-searchbox/gh-searchbox-result.component';
import {GhSearchboxComponent} from './search/gh-searchbox/gh-searchbox.component';

@NgModule({
  imports: [
    CommonModule,
    FlexLayoutModule,
    UdcHomeRoutingModule,
    MaterialModule,
    NgxDatatableModule,
    ReactiveFormsModule,
    SharedModule,
  ],
  declarations: [
    UdcHomeComponent,
    HomeSearchComponent,
    GhSearchboxComponent,
    GhSearchboxResultComponent,
  ],
})
export class UdcHomeModule { }
