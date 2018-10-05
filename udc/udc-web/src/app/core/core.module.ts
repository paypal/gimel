import { NgModule, Optional, SkipSelf } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { HttpModule } from '@angular/http';
import { MaterialModule } from '@angular/material';
import { StoreModule } from '@ngrx/store';
import { StoreDevtoolsModule } from '@ngrx/store-devtools';
import { FlexLayoutModule } from '@angular/flex-layout';
import { ApiService, ConfigService} from './services';
import { reduce } from './store';
import { SharedModule } from '../shared/shared.module';

@NgModule({
  imports: [
    HttpModule,
    CommonModule,
    FormsModule,
    MaterialModule,
    FlexLayoutModule,
    SharedModule,
    StoreModule.provideStore(reduce),
    StoreDevtoolsModule.instrumentOnlyWithExtension(),
  ],
  exports: [],
  providers: [
    ApiService,
    ConfigService,
  ],
})
export class CoreModule {
}
