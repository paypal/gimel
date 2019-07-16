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

import { Component, Input, OnInit } from '@angular/core';
import {Router} from '@angular/router';
import { Subject } from 'rxjs/Subject';
import { Observable } from 'rxjs/Observable';
import { RecursiveSearchService } from './recursive-search.service';
import { CummulativeDataset } from '../../../../udc/catalog/models/catalog-cumulative-dataset';
import {SessionService} from '../../../../core/services/session.service';

@Component({
  selector: 'app-recursive-searchbox',
  templateUrl: './recursive-searchbox.component.html',
  styleUrls: ['./recursive-searchbox.component.scss'],
  providers: [RecursiveSearchService],
})
export class RecursiveSearchboxComponent implements OnInit {
  searchResults: Observable<CummulativeDataset>;
  searching = false;
  searchFailed = false;
  searchTerm: string;
  emptyTerm = false;
  openSearchResult = true;
  searchBoxFocus = false;
  @Input() previousSearchTerm: string;

  private searchTerms = new Subject<string>();

  constructor(private searchService: RecursiveSearchService, private sessionService: SessionService, private router: Router) {
  }

  ngOnInit() {
    this.searchResults = this.searchTerms
      .debounceTime(450)
      .distinctUntilChanged()
      .do(() => {
        this.emptyTerm = false;
        this.searching = true;
        this.searchFailed = false;
        this.openSearchResult = true;
      })
      .switchMap((term) => {
        if (!term) {
          this.emptyTerm = true;
        }
        return term ? this.searchService.search<any>(term) : Observable.of<any>([]);
      })
      .do(() => this.searching = false)
      .catch((error) => {
        // this.searching = false;
        this.searchFailed = true;
        return Observable.of<object>({});
      });

  }

  // Push a search term into the observable stream.
  search(term: string, event: any): void {
    if (event.key === "Enter") {
      this.router.navigate([`/udc/datasets/${term}`])
    }
    this.searchTerms.next(term);
    this.searchTerm = term;
  }

  closeSearch() {
    this.openSearchResult = false;
    this.searchTerms.next('');
  }

  canOpenSearch() {
    if (this.emptyTerm) {
      return false;
    }
    return this.openSearchResult;
  }

  onFocus() {
    this.searchBoxFocus = true;
  }

  onBlur() {
    this.searchBoxFocus = false;
  }
}
