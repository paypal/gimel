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
