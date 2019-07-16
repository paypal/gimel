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

import { createSelector } from 'reselect';
import * as fromSideNav from './sidenav/sidenav.reducer';
import * as fromGlobalHeader from './global-header/gh.reducer';

/**
 * Top level state is a map of keys to inner state types
 */
export interface State {
  sidenav: fromSideNav.State;
  globalHeader: fromGlobalHeader.State;
}

export const reducers = {
  sidenav: fromSideNav.reducer,
  globalHeader: fromGlobalHeader.reducer,
};

/**
 * Query to get side nav related
 */
export const getSideNavState = (state: State) => state.sidenav;

export const getSideNavShow = createSelector(getSideNavState, fromSideNav.getSideNavShow);

/**
 * Query to get Global header related
 */

export const getGlobalHeaderState = (state: State) => state.globalHeader;

export const getGlobalHeaderBanner = createSelector(getGlobalHeaderState, fromGlobalHeader.getGlobalHeaderBanner);

export const getGlobalHeaderBannerHeight = createSelector(getGlobalHeaderState, fromGlobalHeader.getGlobalHeaderBannerHeight);

export const getGlobalHeaderSnackbar = createSelector(getGlobalHeaderState, fromGlobalHeader.getGlobalHeaderSnackbar);

export const getGlobalHeaderTheme = createSelector(getGlobalHeaderState, fromGlobalHeader.getGlobalHeaderTheme);
