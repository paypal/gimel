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

import { animate, style, state, transition, trigger, AnimationMetadata } from '@angular/animations';

export const fadeInOutAnimation: AnimationMetadata =
  trigger('fadeInOutAnimation', [
    transition(':enter', [
      style({ opacity: 0 }),
      animate('0.6s ease-in-out', style({ opacity: 1 })),
    ]),
    transition(':leave', [
      style({ opacity: 1 }),
      animate('0.6s ease-in-out', style({ opacity: 0 })),
    ]),
  ]);

export const scaleUpDownAnimation: AnimationMetadata =
  trigger('scaleUpDownAnimation', [
    state('inactive', style({ transform: 'scale(1)' })),
    state('active', style({ transform: 'scale(1.1)' })),
    transition('inactive <=> active', animate('100ms ease-in')),
    transition('active => inactive', animate('100ms ease-out')),
  ]);

export const expandCollapseAnimation: AnimationMetadata =
  trigger('expandCollapseAnimation', [
      transition(':enter', [
        style({ maxHeight: 0, overflow: 'hidden' }),
        animate('0.5s ease-in-out', style({ maxHeight: '1000px'})),
      ]),
      transition(':leave', [
        style({ maxHeight: '1000px' }),
        animate('0.2s ease-in-out', style({ maxHeight: 0, overflow: 'hidden' })),
      ]),
    ]);

export const slideInSlideOutAnimation: AnimationMetadata =
  trigger('slideInSlideOutAnimation', [
    transition(':enter', [
      style({transform: 'translateX(-100%)'}),
      animate('0.5s ease-in-out', style({
        transform: 'translateX(0)',
      })),
    ]),
    transition(':leave', [
      style({transform: 'translateX(100%)'}),
      animate('0.5s ease-in-out', style({
        transform: 'translateX(0)',
      })),
    ]),
  ]);

export const slideInFromLeftAnimation: AnimationMetadata =
  trigger('slideInFromLeftAnimation', [
    transition(':enter', [
      style({transform: 'translateX(100%)'}),
      animate('0.5s ease-in-out', style({
        transform: 'translateX(0)',
      })),
    ]),
    transition(':leave', [
      style({transform: 'translateX(100%)'}),
      animate('0.5s ease-in-out', style({
        transform: 'translateX(0)',
      })),
    ]),
  ]);

export const zoomInAnimation: AnimationMetadata =
  trigger('zoomInAnimation', [
    transition(':enter', [
      style({transform: 'scale3d(.3, .3, .3)  ', opacity: '0', offset: 0}),
      animate('0.5s ease-in-out', style({transform: 'none', opacity: '1', offset: 1})),
    ]),
  ]);

export const fadeUpDownAnimation: AnimationMetadata =
  trigger('fadeUpDownAnimation', [
    state('void', style({ opacity: 1, transform: 'translateY(0)' })),
    transition('void => *', [
      style({
        transform: 'translateY(100%)',
      }),
      animate(250),
    ]),
    transition('* => void', [
      style({
        transform: 'translateY(0)',
      }),
      animate(250, style({ transform: 'translateY(100%)' })),
    ]),
  ]);

export const heightUpDownAnimation: AnimationMetadata =
  trigger('heightUpDownAnimation', [
    transition('void => *', [
      style({
        height: 0,
      }),
      animate(250, style({ height: '*' })),
    ]),
    transition('* => void', [
      style({
        height: '*',
      }),
      animate(250, style({ height: 0 })),
    ]),
  ]);
