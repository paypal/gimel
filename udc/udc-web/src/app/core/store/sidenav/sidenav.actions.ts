/* tslint:disable:max-classes-per-file */
import { Action } from '@ngrx/store';

import { type } from '../util';

export const ActionTypes = {
  TOGGLE_SIDENAV: type('[SideNav] Toggle'),
  CLOSE_SIDENAV: type('[SideNav] Close'),
};

export class ToggleSideNavAction implements Action {
  type = ActionTypes.TOGGLE_SIDENAV;
}

export class CloseSideNavAction implements Action {
  type = ActionTypes.CLOSE_SIDENAV;
}

export type Actions = ToggleSideNavAction;
