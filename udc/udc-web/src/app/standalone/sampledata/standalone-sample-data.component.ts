import {Component} from '@angular/core';
import {ActivatedRoute} from '@angular/router';
import {CatalogService} from '../../udc/catalog/services/catalog.service';

@Component({
  selector: 'app-standalone-sampledata-profile', templateUrl: './standalone-sample-data.component.html', styleUrls: ['./standalone-sample-data.component.scss'],
})
export class StandaloneSampleDataProfileComponent {

  public dataList = [];
  public headersList = [];
  public rowsList = [];
  public dataLoading = false;
  public datasetName: string;
  public objectId: number;
  public errorInSampleData: boolean;
  public emptySampleData = {};

  constructor(private route: ActivatedRoute, private catalogService: CatalogService) {
    this.setParamsAndGetSampleData();
  }

  getSampleDataDetails() {
    this.rowsList = [];
    this.dataList = [];
    this.headersList = [];
    this.catalogService.getSampleData(this.datasetName, this.objectId.toString())
      .subscribe(data1 => {
        this.dataList = data1;
        this.errorInSampleData = false;
        this.setEmptySampleDataResponse();
        const firstRow = this.dataList[0];
        if (typeof firstRow === 'string') {
          const test = {prop: 'Error StackTrace'};
          this.headersList.push(test);
          let i: number;
          for (i = 0; i < this.dataList.length; i++) {
            const values = this.dataList[i];
            let temp = {};
            const header = this.headersList[0]['prop'];
            const value = values;
            temp[header] = value;
            this.rowsList.push(temp);
          }
          this.dataLoading = false;
        } else {
          if (firstRow) {
            firstRow.forEach(element => {
              const test = {prop: element};
              this.headersList.push(test);
            });
            this.dataList.shift();
            let i: number;
            let j: number;
            for (i = 0; i < this.dataList.length; i++) {
              const values = this.dataList[i];
              let temp = {};
              for (j = 0; j < this.headersList.length; j++) {
                const header = this.headersList[j]['prop'];
                const value = values[j];
                temp[header] = value;
              }
              this.rowsList.push(temp);
            }
          }
          this.dataLoading = false;
        }
      }, error => {
        this.errorInSampleData = true;
        this.setEmptySampleDataResponse();
        this.rowsList = [];
        this.dataLoading = false;
      });

  }

  setEmptySampleDataResponse() {
    if (this.errorInSampleData) {
      this.emptySampleData = {
        emptyMessage: `
    <div>
      <span>Sample data is unavailable for this dataset at the moment.</span>
    </div>`,
      };
    } else {
      this.emptySampleData = {
        emptyMessage: `
    <div>
      <span>Sample data is unavailable for this dataset.</span>
    </div>`,
      };
    }
  }

  setParamsAndGetSampleData() {
    this.route.params.subscribe(params => {
      this.dataLoading = true;
      this.datasetName = params['name'];
      this.objectId = params['id'];
      this.getSampleDataDetails();
    });
  }
}
