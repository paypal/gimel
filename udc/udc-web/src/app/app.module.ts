import {BrowserModule, Title} from '@angular/platform-browser';
import {NgModule} from '@angular/core';
import {BrowserAnimationsModule} from '@angular/platform-browser/animations';
import {MaterialModule} from '@angular/material';
import {FlexLayoutModule} from '@angular/flex-layout';
import {NgxDatatableModule} from '@swimlane/ngx-datatable';
import 'hammerjs';
import {AppRoutingModule} from './app-routing.module';
import {CoreModule} from './core/core.module';
import {SharedModule} from './shared/shared.module';
import {GlobalHeaderModule} from './global-header/global-header.module';
import {AppComponent} from './app.component';
import './rxjs-extensions';
import {HomeComponent} from './home/home.component';
import {HealthCheckModule} from './healthcheck/healthcheck.module';
import {UdcModule} from './udc/udc.module';
import {UdcHomeModule} from './udc_home/udc-home.module';
import {UdcAdminModule} from './admin/admin.module';

@NgModule({
  declarations: [AppComponent, HomeComponent],
  imports: [BrowserModule, BrowserAnimationsModule, CoreModule, MaterialModule, GlobalHeaderModule, SharedModule, AppRoutingModule, FlexLayoutModule, NgxDatatableModule, HealthCheckModule, UdcHomeModule, UdcModule, UdcAdminModule],
  providers: [Title],
  bootstrap: [AppComponent],
})
export class AppModule {
}
