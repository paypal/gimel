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

import { Component, EventEmitter, Input, Output } from '@angular/core';
import { animate, style, transition, trigger } from '@angular/animations';
import { ConfigService } from '../../core/services';

@Component({
  selector: 'app-sidenav-content',
  templateUrl: './sidenav-content.component.html',
  styleUrls: ['./sidenav-content.component.scss'],
  animations: [trigger('zoomFromLeftAnimation', [transition(':enter', [style({
    transform: 'scale(0.8) translate3d(-40px,0,0)',
    opacity: '0',
  }), animate('0.5s ease-in-out', style({transform: 'scale(1)', opacity: '1'}))])])],
})
export class SidenavContentComponent {
  @Input('tabs') tabs: { label: string, link: string };
  @Input() opened = true;
  @Output() close = new EventEmitter();
  public admin: boolean;
  public classificationAdmin: boolean;

  constructor(private config: ConfigService) {
    this.config.getAdminEmitter().subscribe(data => {
      this.admin = data;
    });
    this.admin = this.config.admin;
    this.config.getClassificationAdminEmitter().subscribe(data => {
      this.classificationAdmin = data;
    });
    this.classificationAdmin = this.config.classificationAdmin;
  }

  onClick(): void {
    this.close.emit();
  }
}
