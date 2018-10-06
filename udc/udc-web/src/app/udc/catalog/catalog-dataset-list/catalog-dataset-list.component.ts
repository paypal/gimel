import {
  Component, Input, Output, OnChanges, EventEmitter, ViewChild, SimpleChanges, OnInit
} from '@angular/core';
import {MdSnackBar, MdSnackBarConfig} from '@angular/material';
import {ConfigService} from '../../../core/services';
import {CatalogService} from '../services/catalog.service';
import {Page} from '../models/catalog-list-page';
import {Dataset} from '../models/catalog-dataset';

@Component({
  selector: 'app-catalog-dataset-list-registered', templateUrl: './catalog-dataset-list.component.html',
})
export class CatalogDatabaseListComponent implements OnChanges {
  public loading = false;
  public displayList = new Array<Dataset>();
  private datasetList = [];
  public systemName = '';
  public typeName = '';
  public clusterId = '';
  public datasetStr = 'All';
  public searchString = '';
  page = new Page();
  @ViewChild('catalogDatasetsTable') table: any;

  @Input() project: string;
  @Input() projectType: string;
  @Input() refresh: boolean;
  @Output() loaded: EventEmitter<boolean> = new EventEmitter();

  constructor(private catalogService: CatalogService, private snackbar: MdSnackBar, private config: ConfigService) {
    this.page.pageNumber = 0;
    this.page.size = 20;
  }

  ngOnChanges(changes: SimpleChanges) {
    if (changes.projectType && changes.projectType.currentValue) {
      this.typeName = changes.projectType.currentValue;
    }
    if (changes.project && changes.project.currentValue) {
      this.systemName = changes.project.currentValue;
    }
    if (this.systemName && this.systemName !== '' && this.typeName && this.typeName !== '') {
      this.loadDatasetsFromCatalog({offset: 0});
    }
    if (changes.refresh && !changes.refresh.firstChange) {
      if (this.systemName && this.systemName !== '') {
        this.loadDatasetsFromCatalog({offset: 0});
      }
    }
  }

  searchDatasets() {
    this.datasetStr = this.searchString;
    this.loadDatasetsFromCatalog({offset: 0});
  }

  loadDatasetsFromCatalog(pageInfo) {
    this.loading = true;
    this.datasetList = [];
    this.page.pageNumber = pageInfo.offset;
    this.catalogService.getDataSetListPageable(this.datasetStr, this.systemName, this.typeName, this.page).subscribe(pagedData => {
      this.datasetList = pagedData.data.filter(dataset => dataset.attributesPresent);
      this.page = pagedData.page;
      this.displayList = this.datasetList;
    }, error => {
      this.datasetList = [];
      this.displayList = [];
    }, () => {
      this.loading = false;
      this.loaded.emit(true);
    });
  }

  // search(searchText: string) {
  //   this.displayList = this.datasetList.filter((item, index, array) => {
  //     return item['storageDataSetName'].toLowerCase().search(searchText.toLowerCase()) !== -1 || item['storageSystemName'].toLowerCase().search(searchText.toLowerCase()) !== -1;
  //   });
  //
  // }

  toggleExpandRow(row) {
    this.table.rowDetail.toggleExpandRow(row);
  }

  refreshRow(event: string) {
    // this.loadDatasetsFromCatalog();
    this.loadDatasetsFromCatalog({offset: 0});
  }
}
