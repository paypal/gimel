import {Component, Input, OnInit} from '@angular/core';

import {CatalogService} from '../../services/catalog.service';
import {Schema} from '../../models/catalog-schema';

@Component({
  selector: 'app-catalog-dataset-detail', templateUrl: './catalog-dataset-detail.component.html', styleUrls: ['./catalog-dataset-detail.component.scss'],
})

export class CatalogDatabaseDetailComponent implements OnInit {
  @Input() dataset: string;
  @Input() datasetName: string;
  @Input() project: string;
  public detailsLoading = false;
  public statusData = {};
  public columnList = new Array<Schema>();
  public systemAttributesList = [];
  public objectAttributesList = [];
  public columnClassificationMap = {};

  constructor(private catalogService: CatalogService) {
    this.columnClassificationMap['restricted_columns_class1'] = 'Class 1';
    this.columnClassificationMap['restricted_columns_class2'] = 'Class 2';
    this.columnClassificationMap['restricted_columns_class3_1'] = 'Class 3';
    this.columnClassificationMap['restricted_columns_class3_2'] = 'Class 3';
    this.columnClassificationMap['restricted_columns_class4'] = 'Class 4';
    this.columnClassificationMap['restricted_columns_class5'] = 'Class 5';
    this.columnClassificationMap[''] = 'N/A';
  }

  ngOnInit() {
    this.getDatasetDetails();

  }

  getDatasetDetails() {
    this.detailsLoading = true;
    this.catalogService.getDatasetPendingDetails(this.dataset)
      .subscribe(data => {
        this.statusData = data;
        const tempList = data.objectSchema;
        tempList.forEach(column => {
          const schemaObject = new Schema();
          schemaObject.columnClass = this.columnClassificationMap[column.columnClass];
          schemaObject.columnFamily = column.columnFamily;
          schemaObject.columnIndex = column.columnIndex;
          schemaObject.columnType = column.columnType;
          schemaObject.restrictionStatus = column.restrictionStatus;
          schemaObject.partitionStatus = column.partitionStatus;
          schemaObject.columnName = column.columnName;
          this.columnList.push(schemaObject);
        });
        this.systemAttributesList = data.systemAttributes;
        this.objectAttributesList = data.objectAttributes;
        this.detailsLoading = false;
      }, error => {
        this.statusData = {};
        this.detailsLoading = false;
      });
  }

}
