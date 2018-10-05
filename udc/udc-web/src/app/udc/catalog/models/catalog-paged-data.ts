import {Page} from './catalog-list-page';

export class PagedData<T> {
  data = new Array<T>();
  page = new Page();
}
