import {NgModule} from '@angular/core';
import {RouterModule, Routes} from '@angular/router';

import {StandaloneStageProfileComponent} from './dataset/standalone-dataset.component';
import {StandaloneDatasetSearchComponent} from './search/standalone-dataset-search.component';
import {StandaloneSampleDataProfileComponent} from './sampledata/standalone-sample-data.component';

const standaloneRoutes: Routes = [{
  path: 'dataset/:id',
  component: StandaloneStageProfileComponent,
  data: {title: 'Dataset ', page: 'standalonedataset'},
}, {
  path: 'datasets/:prefix',
  component: StandaloneDatasetSearchComponent,
  data: {title: 'Datasets' , page: 'standalonedatasetprefix'},
}, {
  path: 'sampledata/:name/:id',
  component: StandaloneSampleDataProfileComponent,
  data: {title: 'Dataset SampleData', page: 'standalonesampledata'},
}];

@NgModule({
  imports: [RouterModule.forChild(standaloneRoutes),], exports: [RouterModule,],
})
export class StandaloneRoutingModule {
}
