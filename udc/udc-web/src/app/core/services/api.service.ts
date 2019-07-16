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

import { Injectable } from '@angular/core';
import { Headers, Http, Response, Request, RequestMethod } from '@angular/http';
import { SessionService } from './session.service';
import { Observable } from 'rxjs/Observable';
import { environment } from '../../../environments/environment';

@Injectable()
export class ApiService {

  public serverWithPort = '';
  public currentEnv = '';

  constructor(private http: Http, private session: SessionService) {
    this.currentEnv = environment.profileIndex;
    this.serverWithPort = environment.protocol + environment.devHost + ':' + environment.port;
  }

  get<T>(url: string, options?: object): Observable<T> {
    let requestOptions: any;
    const headers = new Headers({
      Accept: '*/*', Authorization: this.session.getSessionId(),
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
      'Content-Type': 'application/json', Authorization: this.session.getSessionId(),
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
      'Content-Type': 'application/json', Authorization: this.session.getSessionId(),
    });
    const requestOptions = Object.assign({}, {method: RequestMethod.Put, url, body, headers});
    return this.apiCall<T>(new Request(requestOptions));
  }

  putWithoutPayload<T>(url: string): Observable<T> {
    const headers = new Headers({
      'Content-Type': 'application/json', Authorization: this.session.getSessionId(),
    });
    const requestOptions = Object.assign({}, {method: RequestMethod.Put, url, headers});
    return this.apiCall<T>(new Request(requestOptions));
  }

  delete<T>(url: string): Observable<T> {
    const headers = new Headers({
      Accept: 'application/json', Authorization: this.session.getSessionId(),
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
