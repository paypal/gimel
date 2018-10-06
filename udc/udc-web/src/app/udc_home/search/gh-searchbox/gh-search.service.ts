import {Injectable} from '@angular/core';
import {Observable} from 'rxjs/Observable';
import {URLSearchParams} from '@angular/http';

import {ApiService} from '../../../core/services';

@Injectable()
export class GhSearchService {
  private serverWithPort: string;
  private endPointUrl: string;

  constructor(private api: ApiService) {
    this.serverWithPort = api.serverWithPort;
    this.endPointUrl = this.serverWithPort + '/dataSet/dataSets/';
  }

  search<T>(term: string): Observable<T> {
    const params = new URLSearchParams();
    params.set('prefix', term);
    return this.api.get<T>(this.endPointUrl, {params: params});
  }
}
