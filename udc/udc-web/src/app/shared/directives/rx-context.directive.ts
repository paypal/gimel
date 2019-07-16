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
  ChangeDetectorRef,
  Directive,
  EmbeddedViewRef,
  Input,
  OnInit,
  OnDestroy,
  TemplateRef,
  ViewContainerRef,
} from '@angular/core';
import { Observable } from 'rxjs/Observable';

@Directive({
  selector: '[rxContext][rxContextOn]',
})
export class RxContextDirective implements OnInit, OnDestroy {
  @Input() rxContextOn: Observable<any>;

  private viewRef: EmbeddedViewRef<any>;
  private subscription: any;

  constructor(private viewContainer: ViewContainerRef,
              private template: TemplateRef<any>,
              private cdr: ChangeDetectorRef) {
  }

  ngOnInit() {
    if (this.rxContextOn) {
      this.subscription = this.rxContextOn.subscribe(state => {
        if (!this.viewRef) {
          this.viewRef = this.viewContainer
            .createEmbeddedView(this.template, { '$implicit': state });
        } else {
          this.viewRef.context.$implicit = state;
        }
        // this._cdr.detectChanges();
      });
    }
  }

  ngOnDestroy() {
    this.subscription.unsubscribe();
  }
}
