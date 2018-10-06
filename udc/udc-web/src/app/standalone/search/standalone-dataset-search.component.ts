import {Component, Input, ViewChild} from '@angular/core';
import {ActivatedRoute} from '@angular/router';
import {CatalogService} from '../../udc/catalog/services/catalog.service';
import {CummulativeDataset} from '../../udc/catalog/models/catalog-cumulative-dataset';
import {Dataset} from '../../udc/catalog/models/catalog-dataset';

@Component({
  selector: 'app-standalonesearch-profile', templateUrl: './standalone-dataset-search.component.html', styleUrls: ['./standalone-dataset-search.component.scss'],
})
export class StandaloneDatasetSearchComponent {

  public detailsLoading = false;
  public results = new Array<CummulativeDataset>();
  public systemList = [];
  public systemName: string;
  public displayList = new Array<Dataset>();
  public totalList = new Array<Dataset>();
  public trimmedList = new Array<Dataset>();
  public allString = 'All';
  public searchTerm: string;
  @ViewChild('catalogDatasetsTable') table: any;

  constructor(private route: ActivatedRoute, private catalogService: CatalogService) {
    this.getResults();
  }

  getResults() {
    this.systemList = [];
    this.displayList = [];
    this.totalList = [];
    this.results = [];
    this.route.params.subscribe(params => {
      this.searchTerm = params['prefix'];
      this.detailsLoading = true;
      this.resetLists();
      this.systemList.push(this.allString);
      this.catalogService.getDatasetsWithPrefix(this.searchTerm).subscribe(data => {
        this.results = data;
        this.results.forEach(result => {
          result.datasets.forEach(dataset => {
            this.displayList.push(dataset);
            this.totalList.push(dataset);
            this.trimmedList.push(dataset);
            if (!this.systemList.includes(dataset.storageSystemName)) {
              this.systemList.push(dataset.storageSystemName);
            }
          });
        });
        this.systemName = this.allString;
        this.detailsLoading = false;
      });
    });
  }

  resetLists() {
    if (this.totalList.length > 0) {
      this.totalList.splice(0, this.totalList.length);
    }
    if (this.trimmedList.length > 0) {
      this.trimmedList.splice(0, this.trimmedList.length);
    }
    if (this.systemList.length > 0) {
      this.systemList.splice(0, this.systemList.length);
    }
    if (this.displayList.length > 0) {
      this.displayList.splice(0, this.displayList.length);
    }
  }

  toggleExpandRow(row) {
    this.table.rowDetail.toggleExpandRow(row);
  }

  search(searchText: string) {
    this.displayList = this.trimmedList.filter((item) => {
      return item['storageDataSetName'].toLowerCase().search(searchText.toLowerCase()) !== -1 || item['storageSystemName'].toLowerCase().search(searchText.toLowerCase()) !== -1;
    });
  }

  onStorageSystemChange() {
    if (this.systemName !== this.allString) {
      this.trimmedList = this.totalList.filter((item) => {
        return item['storageSystemName'].search(this.systemName) !== -1;
      });
      this.displayList = this.trimmedList;
    } else {
      this.displayList = this.totalList;
      this.trimmedList = this.totalList;
    }

  }
}
