import {Component, OnInit} from '@angular/core';
import {FormGroup, FormBuilder} from '@angular/forms';
import {MdDialogRef} from '@angular/material';
import {CatalogService} from '../../services/catalog.service';

@Component({
  selector: 'app-catalog-sample-data-dialog', templateUrl: './catalog-dataset-sample-data-dialog.component.html', styleUrls: ['./catalog-dataset-sample-data-dialog.component.scss'],
})

export class CatalogDatasetSampleDataDialogComponent implements OnInit {
  heading = '';
  sampleDataForm: FormGroup;
  public datasetName: string;
  public objectId: number;
  public typeName: string;
  public dataList = [];
  public headersList = [];
  public rowsList = [];
  public errorInSampleData: boolean;
  public emptySampleData = {};
  public detailsLoading = false;

  constructor(public dialogRef1: MdDialogRef<CatalogDatasetSampleDataDialogComponent>, private fb: FormBuilder, public catalogService: CatalogService) {
  }

  ngOnInit() {
    this.sampleDataForm = this.fb.group({});
    this.heading = 'Sample Data for ' + this.datasetName;
    this.getSampleDataDetails();
  }

  cancel() {
    this.dialogRef1.close();
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
          this.detailsLoading = false;
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
          this.detailsLoading = false;
        }
      }, error => {
        this.errorInSampleData = true;
        this.setEmptySampleDataResponse();
        this.rowsList = [];
        this.detailsLoading = false;
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
}
