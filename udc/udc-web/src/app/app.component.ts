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

import { AfterContentInit, Component, OnDestroy } from '@angular/core';
import { DomSanitizer } from '@angular/platform-browser';
import { MatIconRegistry } from '@angular/material';
import { OverlayContainer } from '@angular/cdk/overlay';
import { ApiService, ConfigService } from './core/services';
import { UserService } from './udc/catalog/services/user.service';
import {SessionService} from './core/services/session.service';

@Component({
  selector: 'app-root', templateUrl: './app.component.html',
})
export class AppComponent implements AfterContentInit, OnDestroy {

  subscription: any;
  titleSub: any;

  constructor(private api: ApiService, private config: ConfigService, private overlayContainer: OverlayContainer, mdIconRegistry: MatIconRegistry, sanitizer: DomSanitizer, sessionService: SessionService) {
    mdIconRegistry
      .addSvgIcon('jira', sanitizer.bypassSecurityTrustResourceUrl('assets/icons/jira.svg'))
      .addSvgIcon('slack', sanitizer.bypassSecurityTrustResourceUrl('assets/icons/slack.svg'))
      .addSvgIcon('github', sanitizer.bypassSecurityTrustResourceUrl('assets/icons/github-octocat.svg'))
      .addSvgIcon('confluence', sanitizer.bypassSecurityTrustResourceUrl('assets/icons/confluence.svg'));

    overlayContainer.getContainerElement().classList.add('purple-light-theme');
    const user = new UserService(api, config, sessionService);

  }

  ngAfterContentInit() {
    // setTimeout(() => {
    //   if (!this.config.user.alreadySeen) {
    //   this.intro.websiteTour();
    //   }
    // }, 1000);
  }

  ngOnDestroy() {
    this.subscription.unsubscribe();
    this.titleSub.unsubscribe();
  }
}
