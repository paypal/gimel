import {
  Component, OnInit, animate, style, transition, trigger, OnDestroy,
} from '@angular/core';
import {Store} from '@ngrx/store';
import {Observable} from 'rxjs/Observable';
import {Router, NavigationEnd, ActivatedRoute} from '@angular/router';
import {MdSnackBar} from '@angular/material';

import * as fromRoot from '../core/store';
import {ToggleSideNavAction} from '../core/store/sidenav';
import {ConfigService} from '../core/services';

@Component({
  selector: 'app-global-header', templateUrl: './global-header.component.html', styleUrls: ['./global-header.component.scss'], animations: [trigger('pulseAnimation', [transition(':enter', [style({
    width: '50px', height: '50px', top: '15px', left: '15px', borderRadius: '50%', opacity: '0.5', transform: 'scale(1)'}), animate('0.7s ease-in-out', style({transform: 'scale(200)', opacity: '1'}))])])],
})
export class GlobalHeaderComponent implements OnInit, OnDestroy {
  sidenavState = 'close';
  routerEvents$: Observable<any>;
  pulseAnimationDoneFlag = false;
  showCreateAppSnackbar$: Observable<any>;
  subscription: any;
  globalHeaderTheme$: Observable<any>;

  constructor(private store: Store<fromRoot.State>, private router: Router, private activatedRoute: ActivatedRoute, private config: ConfigService, private snackbar: MdSnackBar) {
    this.showCreateAppSnackbar$ = this.store.select(fromRoot.getGlobalHeaderSnackbar);
    this.globalHeaderTheme$ = this.store.select(fromRoot.getGlobalHeaderTheme);
  }

  ngOnInit() {
    this.routerEvents$ = this.router.events
      .filter(event => event instanceof NavigationEnd)
      .map(() => this.activatedRoute)
      .map(route => {
        while (route.firstChild) {
          route = route.firstChild;
        }
        return route;
      })
      .filter(route => route.outlet === 'primary')
      .mergeMap(route => route.data);
  }

  ngOnDestroy() {
    this.subscription.unsubscribe();
  }

  toggleSideNav() {
    if (this.sidenavState === 'close') {
      this.pulseAnimationDoneFlag = false;
      this.sidenavState = 'open';
    } else {
      this.sidenavState = 'close';
    }
    this.store.dispatch(new ToggleSideNavAction());
  }

  animationDone() {
    setTimeout(() => {
      this.pulseAnimationDoneFlag = true;
    });
  }
}
