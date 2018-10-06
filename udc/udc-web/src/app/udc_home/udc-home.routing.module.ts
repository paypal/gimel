import {NgModule} from '@angular/core';
import {RouterModule, Routes} from '@angular/router';
import {HomeSearchComponent} from './search/udc-home-search.component';

const udcHomeRoutes: Routes = [

  {
    path: 'search', component: HomeSearchComponent, data: {title: 'UDC Search', page: 'search'},
  },];
@NgModule({
  imports: [RouterModule.forChild(udcHomeRoutes)], exports: [RouterModule],
})
export class UdcHomeRoutingModule {
}
