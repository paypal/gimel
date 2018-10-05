import {Component, Input, OnInit} from '@angular/core';
import {Subject} from 'rxjs/Subject';
import {Observable} from 'rxjs/Observable';
import {RecursiveSearchService} from './recursive-search.service';
import {CummulativeDataset} from '../../../udc/catalog/models/catalog-cumulative-dataset';

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

  constructor(private searchService: RecursiveSearchService) {
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
