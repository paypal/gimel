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

import {Injectable} from '@angular/core';
import {ApiService} from '../../../core/services/api.service';
import {forkJoin} from 'rxjs/observable/forkJoin';
import {ConfigService} from '../../../core/services/config.service';
import {SessionService} from '../../../core/services/session.service';
import {CatalogService} from './catalog.service';

@Injectable()
export class UserService {

  defaultUserName: string;
  defaultRoleName: string;
  defaultClassificationAdminRoleName: string;
  userFromSessionURL: string;
  udcAdminRole: string;
  udcClassificationAdminRole: string;
  defaultQid: string;
  defaultLocation: string;
  notificationsURL: string;


  constructor(private api: ApiService, private config: ConfigService, private sessionService: SessionService) {
    this.udcAdminRole = 'UDC_ADMIN_ROLE';
    this.udcClassificationAdminRole = 'UDC_CLASSIFICATION_ADMIN';
    this.defaultUserName = 'udcdev';
    this.defaultRoleName = 'UDC_ADMIN_ROLE';
    this.defaultClassificationAdminRoleName = 'UDC_CLASSIFICATION_ADMIN';
    this.defaultQid = 'UDC';
    this.defaultLocation = 'LOCATION';
    this.notificationsURL = api.serverWithPort + '/notification/notifications';
    if (this.api.currentEnv !== 'dev' && this.api.currentEnv !== 'prod') {
      this.userFromSessionURL = api.serverWithPort + '/user/userDetails';
      const user = this.getUserFromSession();
      const notifications = this.getNotificationList();
      forkJoin([user, notifications]).subscribe(results => {
        const userDetails = results[0];
        const notificationList = results[1].filter(notification => notification.activeYN === true);
        this.config.setUserName(userDetails['userName']);
        this.config.setRole(userDetails['roles']);
        const adminStatus: boolean = this.getAdmin(userDetails['roles']);
        this.config.setAdmin(adminStatus);
        const classificationAdminStatus: boolean = this.getClassificationAdmin(userDetails['roles']);
        this.config.setClassificationAdmin(classificationAdminStatus);
        this.config.setQid(userDetails['qid']);
        this.config.setLocation(userDetails['location']);
        this.config.setNotificationCount(notificationList.length);
      }, error => {
      });
    } else {
      this.config.setRole(this.defaultRoleName);
      this.config.setUserName(this.defaultUserName);
      const adminStatus: boolean = this.getAdmin(this.defaultRoleName);
      this.config.setAdmin(adminStatus);
      const classificationAdminStatus: boolean = this.getClassificationAdmin(this.defaultClassificationAdminRoleName);
      this.config.setClassificationAdmin(classificationAdminStatus);
      this.config.setQid(this.defaultQid);
      this.config.setLocation(this.defaultLocation);
      const notifications = this.getNotificationList();
      forkJoin([notifications]).subscribe(results => {
        const notificationList = results[0];
        this.config.setNotificationCount(notificationList.length);
      }, error => {
      });
    }
  }

  getUserFromSession() {
    return this.api.get<Array<any>>(this.userFromSessionURL);
  }

  getNotificationList() {
    return this.api.get<Array<any>>(this.notificationsURL);
  }

  getAdmin(role: string) {
    if (role.search(this.udcAdminRole) === -1) {
      return false;
    } else {
      return true;
    }
  }

  getClassificationAdmin(role: string) {
    if (role.search(this.udcClassificationAdminRole) === -1) {
      return false;
    } else {
      return true;
    }
  }
}
