import {NgModule} from '@angular/core';
import {RouterModule} from '@angular/router';
import {HomeComponent} from './home/home.component';

const appRoutes = [{
  path: '', component: HomeComponent, data: {title: 'Home', page: 'home'},
}, {
  path: 'udc', loadChildren: './udc/udc.module#UdcModule',
}, {
  path: 'home', loadChildren: './udc_home/udc-home.module#UdcHomeModule',
}, {
  path: 'standalone', loadChildren: './standalone/standalone.module#StandaloneModule',
}, {
  path: 'admin', loadChildren: './admin/admin.module#UdcAdminModule',
}, {
  path: 'health', loadChildren: './health/health.module#HealthModule',
}];

@NgModule({
  imports: [RouterModule.forRoot(appRoutes, {useHash: true})],
  exports: [RouterModule],
})

export class AppRoutingModule {
}
