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

import {ChangeDetectionStrategy, Component, ViewChildren, QueryList, EventEmitter, Output} from '@angular/core';
import {MatMenuTrigger} from '@angular/material/menu';
import {Store} from '@ngrx/store';
// import {Observable} from 'rxjs/Observable';
import {ConfigService} from '../../core/services';
import * as fromRoot from '../../core/store';
import {fadeInOutAnimation} from '../../shared/animations/animations';
import {ApiService} from '../../core/services/api.service';
import {ChangeDetectorRef} from '@angular/core';
import {CatalogService} from '../../udc/catalog/services/catalog.service';

@Component({
  selector: 'app-ghtoolbar', templateUrl: './ghtoolbar.component.html', styleUrls: ['./ghtoolbar.component.scss'], changeDetection: ChangeDetectionStrategy.OnPush, animations: [fadeInOutAnimation],
})
export class GhtoolbarComponent {
  @ViewChildren(MatMenuTrigger) triggers: QueryList<MatMenuTrigger>;
  @Output() showNotification = new EventEmitter<string>();
  notificationCount = 0;
  // showBanner$: Observable<any>;
  username: string;
  userUrl: string;
  role: string;
  admin: boolean;
  qid: string;
  portalUrl: string;
  imageUrl: string;
  userImage: string;

  constructor(private api: ApiService, private config: ConfigService, private catalogService: CatalogService, private store: Store<fromRoot.State>, private ref: ChangeDetectorRef) {

    // this.catalogService.getNotificationList().subscribe(data => {
    //   this.config.setNotificationEmitter(data.length);
    // });
    this.portalUrl = 'https://mycorp.com/profile/';
    this.imageUrl = 'https://image.shutterstock.com/image-illustration/creative-illustration-default-avatar-profile-260nw-1400808113.jpg';
    if (this.api.currentEnv !== 'dev' && this.api.currentEnv !== 'prod') {
      this.config.getUserNameEmitter().subscribe(userdata => {
        this.username = userdata;
        this.userUrl = this.portalUrl + this.username;
        this.config.getRoleEmitter().subscribe(roleData => {
          this.role = roleData;
          this.config.getAdminEmitter().subscribe(adminData => {
            this.admin = adminData;
            this.config.getQidEmitter().subscribe(qidData => {
              this.qid = qidData;
              this.userImage = this.imageUrl
              this.config.getNotificationEmitter().subscribe(data => {
                this.notificationCount = data;
                this.ref.detectChanges();
              });
            });
          });
        });
      });
    } else {
      this.username = this.config.userName;
      this.role = this.config.role;
      this.admin = this.config.admin;
      this.qid = this.config.developerQid;
      this.userImage = this.imageUrl
      this.config.getNotificationEmitter().subscribe(data => {
        this.notificationCount = data;
        this.ref.detectChanges();
      });
    }
  }

  openAccountMenu() {
    if (!this.triggers.last.menuOpen) {
      this.triggers.last.openMenu();
    }
  }

  showBanner() {
    this.showNotification.emit(null);
  }

  openFeedbackMenu() {
    if (!this.triggers.first.menuOpen) {
      this.triggers.first.openMenu();
    }
  }
}
