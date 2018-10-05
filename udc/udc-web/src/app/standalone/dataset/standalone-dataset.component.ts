import {Component} from '@angular/core';
import {ActivatedRoute} from '@angular/router';
import {CatalogService} from '../../udc/catalog/services/catalog.service';
import {Dataset} from '../../udc/catalog/models/catalog-dataset';
import {Schema} from '../../udc/catalog/models/catalog-schema';

@Component({
  selector: 'app-standalonestage-profile', templateUrl: './standalone-dataset.component.html', styleUrls: ['./standalone-dataset.component.scss'],
})
export class StandaloneStageProfileComponent {

  datasetId: number;
  dataset: Dataset = new Dataset('', 0, '', null, 0, '', '', '', '', '', '', '', [], [], '');
  public detailsLoading = false;
  public columnList = new Array<Schema>();
  public systemAttributesList = [];
  public objectAttributesList = [];
  public accessControlList = [];
  public dataList = [];
  public headersList = [];
  public rowsList = [];
  public dataLoading = false;
  public datasetname: string;
  public storageSystemId: number;
  public type: string;
  public errorInSampleData: boolean;
  public columnClassificationMap = {};

  messages = {
    emptyMessage: `
    <div>
      <span>Access control policies on this dataset are not available at the moment.</span>
    </div>`,
  };

  kafkaMessages = {
    emptyMessage: `
    <div>
      <span>This dataset has no access control policies implemented.</span>
    </div>`,
  };

  public emptySampleData = {};

  constructor(private route: ActivatedRoute, private catalogService: CatalogService) {
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

  getAccessControls(storageSystemId: number) {
    this.catalogService.getStorageSystem(storageSystemId.toString()).subscribe(data => {
      this.type = data.storageType.storageTypeName;
      const clusterId: number = data.runningClusterId;
      if (this.type === 'Hive') {
        this.catalogService.getClusterById(clusterId).subscribe(clusterData => {
          let listnum: number = 0;
          for (listnum = 0; listnum < this.objectAttributesList.length; listnum++) {
            const stringToReplace = 'hdfs://' + clusterData.clusterName.toLowerCase();
            const attributeValue = this.objectAttributesList[listnum].objectAttributeValue.replace(stringToReplace, '');
            if (attributeValue.includes('/')) {
              this.getAccessForHadoop(attributeValue, 'hdfs', clusterId);
            }
          }
        });
      } else if (this.type === 'Hbase') {
        this.catalogService.getClusterById(clusterId).subscribe(clusterData => {
          let listnum: number = 0;
          for (listnum = 0; listnum < this.objectAttributesList.length; listnum++) {
            const attributeValue = this.objectAttributesList[listnum].objectAttributeValue;
            this.getAccessForHadoop(attributeValue, this.type.toLowerCase(), clusterId);
          }
        });
      } else if (this.type === 'Teradata') {
        let listnum: number = 0;
        for (listnum = 0; listnum < this.objectAttributesList.length; listnum++) {
          const attributeValue = this.objectAttributesList[listnum].objectAttributeValue;
          const databaseName = attributeValue.split('.')[0];
          this.getAccessForTeradata(storageSystemId, databaseName);
        }
      }
    });

  }

  getAccessForTeradata(storageSystemId: number, attributeValue: string) {
    this.catalogService.getTeradataPolicy(storageSystemId.toString(), attributeValue).subscribe(data => {
      if (data && data.length > 0) {
        data.forEach(element => {
          this.accessControlList.push(element);
        });
      }
    });
  }

  getAccessForHadoop(attributeValue: string, type: string, clusterId: number) {
    let tempList = [];
    this.catalogService.getAccessControl(attributeValue, type, clusterId).subscribe(data => {
      if (data && data.length > 0) {
        tempList = data;
        let num: number = 0;
        for (num = 0; num < tempList.length; num++) {
          const temp = tempList[num];
          const policies = temp.policyItems;
          policies.forEach(policy => {
            this.accessControlList.push(policy);
          });

        }
      }
    });
  }

  getSampleDataDetails() {
    this.rowsList = [];
    this.dataList = [];
    this.headersList = [];
    this.catalogService.getSampleData(this.dataset.storageDataSetName, this.dataset.objectSchemaMapId.toString())
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

  getDatasetDetails() {
    this.route.params.subscribe(params => {
      this.detailsLoading = true;
      this.dataLoading = true;
      this.datasetId = params['id'];
      this.accessControlList = [];
      this.columnList = [];
      this.catalogService.getDatasetPendingDetails(this.datasetId.toString())
        .subscribe(data => {
          this.dataset = data;
          this.detailsLoading = false;
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
          this.datasetname = this.dataset.storageDataSetName;
          this.storageSystemId = data.storageSystemId;
          this.getSampleDataDetails();
          this.getAccessControls(this.storageSystemId);
          this.detailsLoading = false;
        }, error => {
          this.dataset = new Dataset('', 0, '', null, 0, '', '', '', '', '', '', '', [], [], '');
          this.detailsLoading = false;
        });
    });
  }
}
