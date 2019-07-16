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
  ChangeDetectionStrategy,
  Component,
  ViewChild,
  ViewChildren,
  QueryList,
  ViewContainerRef,
  Input,
  Output,
  EventEmitter
} from '@angular/core';
import {
  MatMenuTrigger,
} from '@angular/material/menu';
import {
  Portal,
  TemplatePortalDirective,
} from '@angular/cdk/portal';

import {
  Overlay,
  OverlayRef,
  CdkOverlayOrigin,
  OverlayConfig,
} from '@angular/cdk/overlay';

import { ConfigService } from '../../core/services';

@Component({
  selector: 'app-ghsidebar',
  templateUrl: './ghsidebar.component.html',
  styleUrls: ['./ghsidebar.component.scss'],
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class GhsidebarComponent {
  @ViewChildren(MatMenuTrigger) triggers: QueryList<MatMenuTrigger>;
  @ViewChildren(TemplatePortalDirective) templatePortals: QueryList<Portal<any>>;
  @ViewChild(CdkOverlayOrigin) overlayOrigin: CdkOverlayOrigin;

  username: string;
  private overlayRef: OverlayRef;

  @Input() opened = false;
  @Input() sideNavOpened = false;
  @Input() xmarkState = false;

  @Output() onToggleSideNav = new EventEmitter<any>();

  constructor(public overlay: Overlay,
              public viewContainerRef: ViewContainerRef,
              private config: ConfigService) {
    this.username = this.config.userName;
  }

  toggleSideNav() {
    this.onToggleSideNav.emit();
  }

  openHelpMenu() {
    if (!this.overlayRef) {
      const overlayConfig = new OverlayConfig();
      overlayConfig.positionStrategy = this.overlay.position()
        .connectedTo(
          this.overlayOrigin.elementRef,
          { originX: 'start', originY: 'bottom' },
          { overlayX: 'start', overlayY: 'top' });
      overlayConfig.hasBackdrop = true;
      overlayConfig.backdropClass = 'mat-overlay-transparent-backdrop';
      this.overlayRef = this.overlay.create(overlayConfig);
    }

    this.overlayRef.attach(this.templatePortals.first);
    this.overlayRef.backdropClick().subscribe(() => this.overlayRef.detach());
  }

  openAccountMenu() {
    if (!this.triggers.first.menuOpen) {
      this.triggers.first.openMenu();
    }
  }
}
