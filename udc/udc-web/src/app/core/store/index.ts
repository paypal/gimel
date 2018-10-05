import {ActionReducer, combineReducers} from '@ngrx/store';
import {createSelector} from 'reselect';
import * as fromSideNav from './sidenav/sidenav.reducer';
import * as fromGlobalHeader from './global-header/gh.reducer';

/**
 * Top level state is a map of keys to inner state types
 */
export interface State {
  sidenav: fromSideNav.State;
  globalHeader: fromGlobalHeader.State;
}

/**
 * Combine all reducers to make top level reducer
 */
const reducers = {
  sidenav: fromSideNav.reducer, globalHeader: fromGlobalHeader.reducer,
};

const roorReducer: ActionReducer<State> = combineReducers(reducers);

export function reduce(state: any, action: any) {
  return roorReducer(state, action);
}

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
