import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { MaterialModule } from '@angular/material';
import { FormsModule } from '@angular/forms';
import { ReactiveFormsModule } from '@angular/forms';
import { NgxDatatableModule } from '@swimlane/ngx-datatable';
import { StandaloneRoutingModule } from './standalone-routing.module';
import { StandaloneStageProfileComponent } from './dataset/standalone-dataset.component';
import {StandaloneSampleDataProfileComponent} from './sampledata/standalone-sample-data.component';
import {StandaloneDatasetSearchComponent} from './search/standalone-dataset-search.component';
import {CatalogDatasetSearchDetailComponent} from './search/search-detail/catalog-dataset-search-detail.component';
import {CatalogService} from '../udc/catalog/services/catalog.service';
import {SharedModule} from '../shared/shared.module';
import {CatalogDatasetSearchActionComponent} from './search/search-action/catalog-dataset-search-action.component';
import {RecursiveSearchboxComponent} from './search/recursive-search/recursive-searchbox.component';
import {RecursiveSearchboxResultComponent} from './search/recursive-search/recursive-searchbox-result.component';

@NgModule({
  imports: [
    CommonModule,
    MaterialModule,
    FormsModule,
    ReactiveFormsModule,
    StandaloneRoutingModule,
    NgxDatatableModule,
    SharedModule,
  ],
  declarations: [
    StandaloneStageProfileComponent,
    StandaloneSampleDataProfileComponent,
    StandaloneDatasetSearchComponent,
    CatalogDatasetSearchDetailComponent,
    CatalogDatasetSearchActionComponent,
    RecursiveSearchboxComponent,
    RecursiveSearchboxResultComponent,
  ],
  providers: [
    CatalogService,
  ],
})
export class StandaloneModule { }
