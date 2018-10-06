/* tslint:disable:max-classes-per-file */
import { Action } from '@ngrx/store';

import { type } from '../util';

export const ActionTypes = {
  SHOW_BANNER: type('[Global Header] Show Banner'),
  HIDE_BANNER: type('[Global Header] Hide Banner'),
  SET_BANNER_HEIGHT: type('[Global Header] Set Banner Height'),
  SHOW_CREATE_APP_SNACKBAR: type('[Global Header] Show Create App Snackbar'),
  CHANGE_THEME: type('[Global Header] Change Theme'),
};

export class ShowBannerAction implements Action {
  type = ActionTypes.SHOW_BANNER;
}

export class HideBannerAction implements Action {
  type = ActionTypes.HIDE_BANNER;
}

export class SetBannerHeightAction implements Action {
  type = ActionTypes.SET_BANNER_HEIGHT;

   constructor(public payload: number) { }
}

// export class ShowCreateAppSnackbarAction implements Action {
//   type = ActionTypes.SHOW_CREATE_APP_SNACKBAR;
//
//   constructor(public payload: any) { }
// }

export class ChangeThemeAction implements Action {
  type = ActionTypes.CHANGE_THEME;

  constructor(public payload: any) { }
}
