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

import { Injectable } from '@angular/core';
import { Router } from '@angular/router';
import { Location } from '@angular/common';
import { MatDialog, MatDialogRef } from '@angular/material';

import { ApiService } from './api.service';
import { ConfigService } from './config.service';

@Injectable()
export class IntrojsService {
  private introJs: any;
  private intro: any;
  private dashboardVisited = false;
  private detailsVisited = false;
  private originUri: string;

  constructor(private api: ApiService, private config: ConfigService, private location: Location, private router: Router, private dialog: MatDialog) {
    this.introJs = require('intro.js').introJs;
  }

  private start(path: string) {
    if (path.indexOf('/detail') > -1 && !this.dashboardVisited) {
      this.detailsVisited = true;
      this.intro.setOptions({'doneLabel': 'Next page'});
    } else if (path.indexOf('/dashboard') > -1 && !this.detailsVisited) {
      this.dashboardVisited = true;
      this.intro.setOptions({'doneLabel': 'Next page'});
    } else {
      this.intro.setOptions({'doneLabel': 'Thanks'});
    }
    this.intro.oncomplete(this.onComplete.bind(this));
    this.intro.onexit(this.onExit.bind(this));
    this.intro.setOptions({'tooltipPosition': 'auto'});
    this.intro.setOptions({'showStepNumbers': false});
    this.intro.setOptions({'skipLabel': 'Exit'});
    this.intro.start();
  }

  private onComplete() {
    const path = this.location.path();
    // const tmp = path.split('/');
    if (path.indexOf('/detail') > -1 && !this.dashboardVisited) {
      // this.router.navigateByUrl(`/app/${ tmp[2] }/dashboard`).then(() => this.startTour(7));
    } else if (path.indexOf('/dashboard') > -1 && !this.detailsVisited) {
      // this.router.navigateByUrl(`/app/${ tmp[2] }/detail/userstage`).then(() => this.startTour(7));
    } else {
      this.router.navigateByUrl(this.originUri);
      // update user
      // this.setTakenFlag().subscribe();
    }
  }

  private onExit() {
    // TODO: ask user if they want to exit
  }

  private setTakenFlag() {
    return this.api.post(`${ this.config.apiServerEndPoint }/users/${ this.config.userName }`, {tourTaken: true});
  }
}
