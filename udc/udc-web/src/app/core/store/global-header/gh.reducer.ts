import * as gh from './gh.actions';

export interface State {
  showBanner: boolean;
  bannerHeight: number;
  showCreateAppSnackbar: boolean;
  createAppsSnackbarMessage: any;
  theme: string;
}

const initialState: State = {
  showBanner: true,
  bannerHeight: 0,
  showCreateAppSnackbar: false,
  createAppsSnackbarMessage: { },
  theme: 'purple-light-theme',
};

export function reducer(state = initialState, action: any): State {
  switch (action.type) {
    case gh.ActionTypes.SHOW_BANNER: {
      return {
        showBanner: true,
        bannerHeight: state.bannerHeight,
        showCreateAppSnackbar: state.showCreateAppSnackbar,
        createAppsSnackbarMessage: state.createAppsSnackbarMessage,
        theme: state.theme,
      };
    }
    case gh.ActionTypes.HIDE_BANNER: {
      return {
        showBanner: false,
        bannerHeight: state.bannerHeight,
        showCreateAppSnackbar: state.showCreateAppSnackbar,
        createAppsSnackbarMessage: state.createAppsSnackbarMessage,
        theme: state.theme,
      };
    }
    case gh.ActionTypes.SET_BANNER_HEIGHT: {
      return {
        showBanner: state.showBanner,
        bannerHeight: action.payload,
        showCreateAppSnackbar: state.showCreateAppSnackbar,
        createAppsSnackbarMessage: state.createAppsSnackbarMessage,
        theme: state.theme,
      };
    }
    case gh.ActionTypes.SHOW_CREATE_APP_SNACKBAR: {
      return {
        showBanner: state.showBanner,
        bannerHeight: state.bannerHeight,
        showCreateAppSnackbar: action.payload.showCreateAppSnackbar,
        createAppsSnackbarMessage: {
          applicationId: action.payload.applicationId,
        },
        theme: state.theme,
      };
    }
    case gh.ActionTypes.CHANGE_THEME: {
      return {
        showBanner: state.showBanner,
        bannerHeight: state.bannerHeight,
        showCreateAppSnackbar: state.showCreateAppSnackbar,
        createAppsSnackbarMessage: state.createAppsSnackbarMessage,
        theme: action.payload,
      };
    }
    default: {
      return state;
    }
  }
}

export const getGlobalHeaderBanner = (state: State) => state.showBanner;

export const getGlobalHeaderBannerHeight = (state: State) => state.bannerHeight;

export const getGlobalHeaderSnackbar = (state: State) => {
  return {
    showCreateAppSnackbar: state.showCreateAppSnackbar,
    createAppsSnackbarMessage: state.createAppsSnackbarMessage,
  };
};

export const getGlobalHeaderTheme = (state: State) => state.theme;
