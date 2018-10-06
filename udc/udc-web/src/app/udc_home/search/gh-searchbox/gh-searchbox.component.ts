import {Component, OnInit} from '@angular/core';
import {Store} from '@ngrx/store';
import {Subject} from 'rxjs/Subject';
import {Observable} from 'rxjs/Observable';
import {GhSearchService} from './gh-search.service';
import * as fromRoot from '../../../core/store';
import {CummulativeDataset} from '../../../udc/catalog/models/catalog-cumulative-dataset';

@Component({
  selector: 'app-gh-searchbox',
  templateUrl: './gh-searchbox.component.html',
  styleUrls: ['./gh-searchbox.component.scss'],
  providers: [GhSearchService],
})
export class GhSearchboxComponent implements OnInit {
  searchResults: Observable<CummulativeDataset>;
  searching = false;
  searchFailed = false;
  searchTerm: string;
  emptyTerm = false;
  openSearchResult = true;
  searchBoxFocus = false;

  private searchTerms = new Subject<string>();

  constructor(private searchService: GhSearchService, private store: Store<fromRoot.State>) {
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
  search(term: string): void {
    this.searchTerms.next(term);
    this.searchTerm = term;
  }

  closeSearch(searchInput) {
    this.openSearchResult = false;
    searchInput.value = '';
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
