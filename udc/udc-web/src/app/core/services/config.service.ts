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

import {EventEmitter, Injectable} from '@angular/core';
import {MatSnackBarConfig} from '@angular/material';

@Injectable()
export class ConfigService {
  developerQid: string;
  userName: string;
  apiServerEndPoint: string;
  qid: string;
  location: string;
  notificationCount: number;
  role: string;
  admin: boolean;
  classificationAdmin: boolean;
  user: any;
  snackBarConfig = new MatSnackBarConfig();
  apiConfig: { [key: string]: string };
  gcpConsoleURL: string;
  userNameEmitter: EventEmitter<string> = new EventEmitter();
  qidEmitter: EventEmitter<string> = new EventEmitter();
  locationEmitter: EventEmitter<string> = new EventEmitter();
  roleEmitter: EventEmitter<string> = new EventEmitter();
  adminEmitter: EventEmitter<boolean> = new EventEmitter();
  classificationAdminEmitter: EventEmitter<boolean> = new EventEmitter();
  notificationEmitter: EventEmitter<number> = new EventEmitter();

  constructor() {
    this.developerQid = 'XXXXX';
    this.snackBarConfig.duration = 15000;
    this.gcpConsoleURL = 'https://console.cloud.google.com/home/dashboard';
  }

  public setNotificationCount(count: number) {
    this.notificationCount = count;
    this.notificationEmitter.emit(this.notificationCount);
  }

  public setUserName(userName: string) {
    this.userName = userName;
    this.userNameEmitter.emit(this.userName);
  }

  public setQid(qid: string) {
    this.qid = qid;
    this.qidEmitter.emit(this.qid);
  }

  public setLocation(location: string) {
    this.location = location;
    this.locationEmitter.emit(this.location);
  }

  public setRole(role: string) {
    this.role = role;
    this.roleEmitter.emit(this.role);
  }

  public setAdmin(admin: boolean) {
    this.admin = admin;
    this.adminEmitter.emit(this.admin);
  }

  public setClassificationAdmin(classificationAdmin: boolean) {
    this.classificationAdmin = classificationAdmin;
    this.classificationAdminEmitter.emit(this.classificationAdmin);
  }

  public getUserNameEmitter() {
    return this.userNameEmitter;
  }

  public getNotificationEmitter() {
    return this.notificationEmitter;
  }

  public getRoleEmitter() {
    return this.roleEmitter;
  }

  public getQidEmitter() {
    return this.qidEmitter;
  }

  public getAdminEmitter() {
    return this.adminEmitter;
  }

  public getClassificationAdminEmitter() {
    return this.classificationAdminEmitter;
  }

  public get(key: string): string {
    return this.apiConfig[key];
  }

}
