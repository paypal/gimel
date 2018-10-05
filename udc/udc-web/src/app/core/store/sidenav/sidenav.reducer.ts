import * as sidenav from './sidenav.actions';

export interface State {
  show: boolean;
}

const initialState: State = {
  show: false,
};

export function reducer(state = initialState, action: any): State {
  switch (action.type) {
    case sidenav.ActionTypes.TOGGLE_SIDENAV: {
      return {
        show: !state.show,
      };
    }
    case sidenav.ActionTypes.CLOSE_SIDENAV: {
      return {
        show: false,
      };
    }
    default: {
      return state;
    }
  }
}

export const getSideNavShow = (state: State) => state.show;
