import {Injectable} from '@angular/core';
import {Headers, Http, Response, Request, RequestMethod} from '@angular/http';

// import {SessionService} from './session.service';
import {Observable} from 'rxjs/Observable';
import {environment} from '../../../environments/environment';

@Injectable()
export class ApiService {

  public serverWithPort = '';
  public currentEnv = '';

  constructor(private http: Http) {
    this.currentEnv = environment.profileIndex;
    this.serverWithPort = environment.protocol + environment.devHost + ':' + environment.port;
  }

  get<T>(url: string, options?: object): Observable<T> {
    let requestOptions: any;
    const headers = new Headers({
      Accept: '*/*',
    });

    if (options) {
      requestOptions = Object.assign({}, options, {method: RequestMethod.Get, url, headers});
    } else {
      requestOptions = Object.assign({}, {method: RequestMethod.Get, url, headers});
    }
    return this.apiCall<T>(new Request(requestOptions));
  }

  post<T>(url: string, payload: any, options?: any): Observable<T> {
    const body = JSON.stringify(payload);
    const headers = new Headers({
      'Content-Type': 'application/json',
    });

    let requestOptions: any;
    if (options && options.headers) {
      requestOptions = Object.assign({}, options, {method: RequestMethod.Post, url, body});
    } else if (options) {
      requestOptions = Object.assign({}, options, {method: RequestMethod.Post, url, body, headers});
    } else {
      requestOptions = Object.assign({}, {method: RequestMethod.Post, url, body, headers});
    }

    return this.apiCall<T>(new Request(requestOptions));
  }


  put<T>(url: string, payload: any): Observable<T> {
    const body = JSON.stringify(payload);
    const headers = new Headers({
      'Content-Type': 'application/json',
    });
    const requestOptions = Object.assign({}, {method: RequestMethod.Put, url, body, headers});
    return this.apiCall<T>(new Request(requestOptions));
  }

  putWithoutPayload<T>(url: string): Observable<T> {
    const headers = new Headers({
      'Content-Type': 'application/json',
    });
    const requestOptions = Object.assign({}, {method: RequestMethod.Put, url, headers});
    return this.apiCall<T>(new Request(requestOptions));
  }

  delete<T>(url: string): Observable<T> {
    const headers = new Headers({
      Accept: 'application/json',
    });
    const requestOptions = Object.assign({}, {method: RequestMethod.Delete, url, headers});
    return this.apiCall<T>(new Request(requestOptions));
  }

  private apiCall<T>(request: Request): Observable<T> {
    return this.http.request(request)
    // .timeout(60000)
      .map((r: Response) => {
        const res = r.json();
        if (res.result !== undefined) {
          return res.result;
        }
        if (res['status'] === 'FAILURE') {
          if (res['error'].errorList.length !== 0) {
            throw(res['error'].errorList[0].message);
          } else if (res['error'].exceptionMessage) {
            console.error(res);
            throw(res['error'].exceptionMessage);
          } else {
            console.error(res);
            throw(`Unexpected Error`);
          }
        } else {
          return res as T;
        }
      }).catch(error => this.handleError(error));
  }

  private handleError(error: Response | any) {
    // let errMsg: string;
    if (error instanceof Response) {
      const body = error.json() || '';
      const err = body.error || JSON.stringify(body);
      return Observable.throw(body);
    } else {
      return Observable.throw(error);
    }
  }
}
