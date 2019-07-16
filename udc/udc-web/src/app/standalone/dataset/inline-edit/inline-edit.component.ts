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

import {Component, Input, Optional, Host, OnInit} from '@angular/core';
import {SatPopover} from '@ncstate/sat-popover';
import {filter} from 'rxjs/operators/filter';
import {CatalogService} from '../../../udc/catalog/services/catalog.service';
import {environment} from '../../../../environments/environment';
import {ConfigService} from '../../../core/services/config.service';
import {forkJoin} from 'rxjs/observable/forkJoin';
import {TeradataDescription} from '../../../admin/models/catalog-teradata-description';
import {TeradataObjectDescription} from '../../../admin/models/catalog-teradata-object-description';
import {DatasetDescription} from '../../../admin/models/catalog-dataset-description';
import {DatasetObjectDescription} from '../../../admin/models/catalog-dataset-object-description';
import {SessionService} from '../../../core/services/session.service';


@Component({
  selector: 'inline-edit', styleUrls: ['inline-edit.component.scss'], template: `
    <form (ngSubmit)="onSubmit()">
      <div class="mat-subheading-2">Add a comment</div>

      <mat-form-field style="width: 600px;">
        <textarea matInput maxLength="1000" name="comment" [(ngModel)]="comment" cdkTextareaAutosize
                  cdkAutosizeMinRows="20"
                  cdkAutosizeMaxRows="50"></textarea>
        <mat-hint align="end">{{comment?.length || 0}}/1000</mat-hint>
      </mat-form-field>

      <div class="actions">
        <button mat-button type="button" color="primary" (click)="onCancel()">CANCEL</button>
        <button mat-button type="submit" color="primary">SAVE</button>
      </div>
    </form>
  `
})
export class InlineEditComponent implements OnInit {

  public username = '';
  public providerName = 'USER';

  @Input()
  get systemId(): number {
    return this._storageSystemId;
  }

  set systemId(x: number) {
    this._systemId = this._storageSystemId = x;
  }

  @Input()
  get datasetId(): number {
    return this._storageDatasetId;
  }

  set datasetId(x: number) {
    this._datasetId = this._storageDatasetId = x;
  }

  @Input()
  get value(): string {
    return this._value;
  }

  set value(x: string) {
    this.comment = this._value = x;
  }

  @Input()
  get systemName(): string {
    return this._storageSystemName;
  }

  set systemName(x: string) {
    this._systemName = this._storageSystemName = x;
  }

  @Input()
  get columnName(): string {
    return this._storageColumnName;
  }

  set columnName(x: string) {
    this._columnName = this._storageColumnName = x;
  }

  @Input()
  get columnDataType(): string {
    return this._storageColumnType;
  }

  set columnDataType(x: string) {
    this._columnType = this._storageColumnType = x;
  }

  @Input()
  get objectId(): number {
    return this._storageObjectId;
  }

  set objectId(x: number) {
    this._objectId = this._storageObjectId = x;
  }

  private objectType = '';
  private _value = '';
  private _storageSystemName = '';
  private _storageColumnName = '';
  private _storageColumnType = '';
  private _storageDatasetId = 0;
  private _storageObjectId = 0;
  private _storageSystemId = 0;

  /** Form model for the input. */
  comment = '';
  _systemName = '';
  _columnName = '';
  _columnType = '';
  _datasetId = 0;
  _objectId = 0;
  _systemId = 0;

  constructor(@Optional() @Host() public popover: SatPopover, private catalogService: CatalogService, private config: ConfigService, private sessionService: SessionService) {
    this.config.getUserNameEmitter().subscribe(data => {
      this.username = data;
    });
    this.username = this.config.userName;
  }

  ngOnInit() {
    if (this.popover) {
      this.popover.closed.pipe(filter(val => val == null))
        .subscribe(() => this.comment = this.value || '');
      this.popover.closed.pipe(filter(val => val == null))
        .subscribe(() => this._datasetId = this._storageDatasetId || 0);
      this.popover.closed.pipe(filter(val => val == null))
        .subscribe(() => this._systemId = this._storageSystemId || 0);
    }
  }

  onSubmit() {
    const objectDetails = this.catalogService.getObjectDetails(this.objectId.toString());


    // check to see if the dataset is Teradata or anything else
    if (this.systemName.indexOf('Teradata') > -1) {
      forkJoin([objectDetails]).subscribe(objectResult => {
        const objectDescriptionDetails = this.catalogService.getDescriptionForTeradata(this.systemId.toString(), objectResult[0].containerName, objectResult[0].objectName, this.providerName);
        forkJoin([objectDescriptionDetails]).subscribe(objectDescriptionResult => {
          // check to see if the description is present for dataset or not
          if (objectDescriptionResult[0].schemaDatasetMapId === 0) {
            // dataset description is missing
            if (objectResult[0].containerName.toLowerCase().indexOf('view') > -1) {
              this.objectType = 'VIEW';
            } else {
              this.objectType = 'TABLE';
            }
            // post dataset description to pc_schema_dataset_map table
            const postDescriptionObject = new TeradataObjectDescription(this.datasetId, this.systemId, this.objectType, objectResult[0].objectName, objectResult[0].containerName, 'N/A', this.username, this.username, this.providerName);
            this.catalogService.insertTeradataDescription(postDescriptionObject).subscribe(insertedData => {
              const schemaDatasetMapId = insertedData.schemaDatasetMapId;
              // post dataset column description to pc_schema_dataset_column_map table
              const postColumnDescObject = new TeradataDescription(schemaDatasetMapId, this.columnName, this.columnName, this.columnDataType, this.comment, this.username, this.username);
              this.catalogService.insertTeradataColumnDescription(postColumnDescObject).subscribe();
            });
          } else {
            // dataset description is present hence we will skip inserting dataset description to pc_schema_dataset_map table
            const objectColumnDescriptionDetails = this.catalogService.getColumnDescriptionForTeradata(objectDescriptionResult[0].schemaDatasetMapId.toString(), this.columnName, this.columnDataType);
            // check if column description is present for the current column
            forkJoin([objectColumnDescriptionDetails]).subscribe(objectColumnDescriptionResult => {

              if (objectColumnDescriptionResult[0].schemaDatasetColumnMapId === 0) {
                // column description if missing and hence we will insert into pc_schema_dataset_column_map table
                const postColumnDescriptionObject = new TeradataDescription(objectDescriptionResult[0].schemaDatasetMapId, this.columnName, this.columnName, this.columnDataType, this.comment, this.username, this.username);
                this.catalogService.insertTeradataColumnDescription(postColumnDescriptionObject).subscribe();
              } else {
                // column description is not missing and hence we will update into pc_schema_dataset_column_map table
                const putColumnDescriptionObject = new TeradataDescription(objectDescriptionResult[0].schemaDatasetMapId, this.columnName, this.columnName, this.columnDataType, this.comment, objectDescriptionResult[0].createdUser, this.username);
                putColumnDescriptionObject.schemaDatasetColumnMapId = objectColumnDescriptionResult[0].schemaDatasetColumnMapId;
                this.catalogService.updateTeradataColumnDescription(putColumnDescriptionObject).subscribe();
              }
            }, objectColumnDescriptionError => {
            });
          }
        }, objectDescriptionError => {
        });
      }, objectError => {
      });
    } else {
      // check to see if the dataset is not Teradata
      forkJoin([objectDetails]).subscribe(objectResult => {
        const objectDescriptionDetails = this.catalogService.getDescriptionForDataset(this.systemId.toString(), objectResult[0].containerName, objectResult[0].objectName, this.providerName);
        forkJoin([objectDescriptionDetails]).subscribe(objectDescriptionResult => {
          // check to see if the description is present for dataset or not
          if (objectDescriptionResult[0].bodhiDatasetMapId === 0) {
            // dataset description is missing and henceforth ost dataset description to pc_dataset_description_map table
            const postDescriptionObject = new DatasetObjectDescription(this.datasetId, this.systemId, this.providerName, objectResult[0].objectName, objectResult[0].containerName, 'N/A', this.username, this.username);
            postDescriptionObject.storageSystemName = this.systemName;
            this.catalogService.insertDatasetDescription(postDescriptionObject).subscribe(insertedData => {
              const bodhiDatasetMapId = insertedData.bodhiDatasetMapId;
              // post dataset column description to pc_dataset_column_description_map table
              const postColumnDescObject = new DatasetDescription(bodhiDatasetMapId, this.columnName, this.comment, this.username, this.username);
              this.catalogService.insertDatasetColumnDescription(postColumnDescObject).subscribe();
            });
          } else {
            // dataset description is present hence we will skip inserting dataset description to pc_dataset_description_map table
            const objectColumnDescriptionDetails = this.catalogService.getColumnDescriptionForDataset(objectDescriptionResult[0].bodhiDatasetMapId.toString(), this.columnName);
            // check if column description is present for the current column
            forkJoin([objectColumnDescriptionDetails]).subscribe(objectColumnDescriptionResult => {

              if (objectColumnDescriptionResult[0].bodhiDatasetColumnMapId === 0) {
                // column description if missing and hence we will insert into pc_schema_dataset_column_map table
                const postColumnDescriptionObject = new DatasetDescription(objectDescriptionResult[0].bodhiDatasetMapId, this.columnName, this.comment, this.username, this.username);
                this.catalogService.insertDatasetColumnDescription(postColumnDescriptionObject).subscribe();
              } else {
                // column description is not missing and hence we will update into pc_schema_dataset_column_map table
                const putColumnDescriptionObject = new DatasetDescription(objectDescriptionResult[0].bodhiDatasetMapId, this.columnName, this.comment, objectDescriptionResult[0].createdUser, this.username);
                putColumnDescriptionObject.bodhiDatasetColumnMapId = objectColumnDescriptionResult[0].bodhiDatasetColumnMapId;
                this.catalogService.updateDatasetColumnDescription(putColumnDescriptionObject).subscribe();
              }
            }, objectColumnDescriptionError => {
            });
          }
        }, objectDescriptionError => {
        });
      }, objectError => {
      });
    }
    if (this.popover) {
      this.popover.close(this.comment);
    }
  }

  onCancel() {
    if (this.popover) {
      this.popover.close();
    }
  }
}
