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

import {
  Component, OnInit, OnDestroy,
} from '@angular/core';
import {
  animate, style, transition, trigger
} from '@angular/animations';
import {Store} from '@ngrx/store';
import {Observable} from 'rxjs/Observable';
import {Router, NavigationEnd, ActivatedRoute} from '@angular/router';
import {MatSnackBar} from '@angular/material';

import * as fromRoot from '../core/store';
import {ToggleSideNavAction} from '../core/store/sidenav';
import {ConfigService} from '../core/services';
import {CatalogService} from '../udc/catalog/services/catalog.service';
import {SessionService} from '../core/services/session.service';

@Component({
  selector: 'app-global-header', templateUrl: './global-header.component.html', styleUrls: ['./global-header.component.scss'], animations: [trigger('pulseAnimation', [transition(':enter', [style({
    width: '50px', height: '50px', top: '15px', left: '15px', borderRadius: '50%', opacity: '0.5', transform: 'scale(1)',
  }), animate('0.7s ease-in-out', style({transform: 'scale(200)', opacity: '1'}))])])],
})
export class GlobalHeaderComponent implements OnInit, OnDestroy {
  sidenavState = 'close';
  routerEvents$: Observable<any>;
  pulseAnimationDoneFlag = false;
  showCreateAppSnackbar$: Observable<any>;
  subscription: any;
  globalHeaderTheme$: Observable<any>;
  notificationList = [];

  constructor(private store: Store<fromRoot.State>, private router: Router, private activatedRoute: ActivatedRoute, private config: ConfigService, private snackbar: MatSnackBar, private catalogService: CatalogService, private sessionService: SessionService) {
    this.getActiveNotifications();
    this.showCreateAppSnackbar$ = this.store.select(fromRoot.getGlobalHeaderSnackbar);
    this.globalHeaderTheme$ = this.store.select(fromRoot.getGlobalHeaderTheme);
    store.subscribe((data) => {
      if (data && data.sidenav && !data.sidenav.show) {
        this.sidenavState = 'close';
      }
    }, error => console.error);
  }

  getActiveNotifications() {
    this.notificationList = [];
    this.catalogService.getNotificationList().subscribe(data => {
      data.map(element => {
        if (element.activeYN === true) {
          element.closeBannerFlag = false;
          this.notificationList.push(element);
        }
      });

    }, error => {
      this.notificationList = [];
    });
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

  closeBanner(notification) {
    notification.closeBannerFlag = false;
  }

  openReferenceUrl(url) {

  }

  showBanner() {
    this.notificationList.forEach(notification => {
      notification.closeBannerFlag = true;
    });
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
