import { animate, AnimationEntryMetadata, style, state, transition, trigger } from '@angular/core';

export const fadeInOutAnimation: AnimationEntryMetadata =
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

export const scaleUpDownAnimation: AnimationEntryMetadata =
  trigger('scaleUpDownAnimation', [
    state('inactive', style({ transform: 'scale(1)' })),
    state('active', style({ transform: 'scale(1.1)' })),
    transition('inactive <=> active', animate('100ms ease-in')),
    transition('active => inactive', animate('100ms ease-out')),
  ]);

export const expandCollapseAnimation: AnimationEntryMetadata =
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

export const slideInSlideOutAnimation: AnimationEntryMetadata =
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

export const slideInFromLeftAnimation: AnimationEntryMetadata =
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

export const zoomInAnimation: AnimationEntryMetadata =
  trigger('zoomInAnimation', [
    transition(':enter', [
      style({transform: 'scale3d(.3, .3, .3)  ', opacity: '0', offset: 0}),
      animate('0.5s ease-in-out', style({transform: 'none', opacity: '1', offset: 1})),
    ]),
  ]);

export const fadeUpDownAnimation: AnimationEntryMetadata =
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

export const heightUpDownAnimation: AnimationEntryMetadata =
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
