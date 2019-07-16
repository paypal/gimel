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

import {
  Component, Input, Output, EventEmitter, ViewChild, OnInit, OnDestroy
} from '@angular/core';
import { MatSelect, MatSnackBar } from '@angular/material';
import { MatDialog, MatDialogConfig, MatDialogRef } from '@angular/material';
import { CatalogDatasetTimelineDialogComponent } from './timeline-dialog/catalog-dataset-timeline-dialog';
import { CatalogService } from '../../../app/udc/catalog/services/catalog.service';
import { ConfigService, SessionService } from '../../../app/core/services';
import { environment } from '../../../environments/environment';
import { FormControl } from '@angular/forms';
import { ReplaySubject, Subject } from 'rxjs';
import { takeUntil } from 'rxjs/operators';
import { forEach } from '@angular/router/src/utils/collection';

@Component({
  selector: 'app-catalog-dataset-timeline',
  templateUrl: './catalog-dataset-timeline.component.html',
  styleUrls: ['./catalog-dataset-timeline.component.scss'],
})
export class CatalogDatabaseTimelineComponent implements OnInit, OnDestroy {
  public changeLogs = [];
  public filterList = [];
  public timelineEnumMap = {};
  public projectTimelineDimensions = [];
  flag = false;
  displayList = [];
  dialogViewConfig: MatDialogConfig = {width: '900px', height: '500px'};
  @Input() datasetId: number;
  filterListLoaded = false;
  @Output() eventCount: EventEmitter<number> = new EventEmitter();
  public filtersCtrl: FormControl = new FormControl();
  public timelineFilterCtrl: FormControl = new FormControl();
  public filteredTimeline: ReplaySubject<Array<any>> = new ReplaySubject<Array<any>>(1);
  @ViewChild('multiSelect') multiSelect: MatSelect;
  protected _onDestroy = new Subject<void>();
  public timelineDimensionDesc = [];
  public addedTag = '';
  public user: any;
  constructor(private catalogService: CatalogService, private snackbar: MatSnackBar, private config: ConfigService, private dialog: MatDialog, private sessionService: SessionService) {

  }

// Get the timeline Dimensions List in Timeline Filter Drop Down
  loadTimelineDimensions() {
    this.filterListLoaded = false;
    this.catalogService.getTimelineDimensions().subscribe(data => {

        // Construct TimelineEnum HashMap with TimelineDimension description as key and its flag and color as value.
        Object.keys(data).forEach(key => {
        this.timelineDimensionDesc.push(key);
        this.timelineEnumMap[key] = data[key];
      });
    }, error => {
      this.filterListLoaded = true;
    }, () => {

      // Sort Timeline Dimensions alphabetically in ascending order
      this.timelineDimensionDesc.push('ALL');
      this.projectTimelineDimensions = this.timelineDimensionDesc;
      this.displayList = this.projectTimelineDimensions.sort((a, b): number => {
        return a > b ? 1 : -1;
      });
      this.filterListLoaded = true;
      // Initially check all the Dimensions in multi-checkbox drop down
      this.filtersCtrl.setValue([this.displayList[0]]);
      // this.filtersCtrl.setValue([ this.datasetDisplayList[0], this.datasetDisplayList[1], this.datasetDisplayList[2], this.datasetDisplayList[3], this.datasetDisplayList[4], this.datasetDisplayList[5], this.datasetDisplayList[6], this.datasetDisplayList[7]]);
      this.filteredTimeline.next(this.displayList.slice());
    });
    this.timelineFilterCtrl.valueChanges
      .pipe(takeUntil(this._onDestroy))
      .subscribe(() => {
        this.filterTimelineDimensions();
      });
  }

  ngOnInit() {
    this.loadTimelineDimensions();
    this.loadTimelineChangeLogs();

  }
  onDotClick() {
    event.stopPropagation();
  }
  openDialog(log: object) {
    let dialogRef: MatDialogRef<CatalogDatasetTimelineDialogComponent>;
    dialogRef = this.dialog.open(CatalogDatasetTimelineDialogComponent, this.dialogViewConfig);
    dialogRef.componentInstance.changeType = log['changeColumnType'];
    dialogRef.componentInstance.prevValue = log['columnPrevVal'];
    dialogRef.componentInstance.updatedValue = log['columnCurrVal'];
    dialogRef.componentInstance.username = log['username'];
    dialogRef.componentInstance.updatedTimestamp = log['dialogTimestampText'];
    dialogRef.componentInstance.status = log['status'];
    dialogRef.componentInstance.modifiedColumn = log['changeColumn'];
    dialogRef.componentInstance.modifiedValue = log['changeValue'];
    dialogRef.componentInstance.heading = log['heading'];
    dialogRef.componentInstance.addedTag = log['addedTag'];
    dialogRef.componentInstance.color = log['addedTagColor'];
    dialogRef.componentInstance.ownerName = log['ownerName'];
    dialogRef.componentInstance.addedOwner = log['addedOwner'];
    dialogRef.componentInstance.deletedOwner = log['deletedOwner'];
    dialogRef.afterClosed().subscribe();
  }
  ngOnDestroy() {
    this._onDestroy.next();
    this._onDestroy.complete();
  }
  loadTimelineChangeLogs() {
    this.changeLogs = [];
    this.catalogService.getChangeLogByDatasetURL(this.datasetId.toString()).subscribe(data => {
      data.map(element => {
        const columnPrevVal = JSON.parse(element.columnPrevValInString);
        const columnCurrVal = JSON.parse(element.columnCurrValInString);
        const changeType = element.storageDatasetChangeType;
        const modifiedTimelineDimension = element.changeColumnType;
        const timestamp = element.updatedTimestamp.split(' ');
        element.dialogTimestampText = timestamp[0] + ' at ' + timestamp[1];
        element.imageUrl = 'https://image.shutterstock.com/image-illustration/creative-illustration-default-avatar-profile-260nw-1400808113.jpg';
        if (changeType === 'C' && modifiedTimelineDimension === 'OWNERSHIP') {
          element.timelineRowText = 'claimed on ' + timestamp[0] + ' at ' + timestamp[1];
        } else if (changeType === 'C') {
          element.timelineRowText = 'created on ' +  timestamp[0] + ' at ' + timestamp[1];
        } else if (changeType === 'M') {
          element.timelineRowText = 'modified on ' + timestamp[0] + ' at ' + timestamp[1];
        }  else if (changeType === 'D') {
          element.timelineRowText = 'deleted on ' + timestamp[0] + ' at ' + timestamp[1];
        }
        switch (modifiedTimelineDimension) {
          case 'DATASET':
            this.datasetNameChanges(columnPrevVal, columnCurrVal, element);
            break;
          case 'OBJECT_DESC':
            this.datasetDescriptionChanges(columnPrevVal, columnCurrVal, element);
            break;
          case 'TAG':
            this.tagChanges(columnPrevVal, columnCurrVal, element);
            break;
          case 'COLUMN_DESC' :
            this.datasetColumnDescChanges(columnPrevVal, columnCurrVal, element);
            break;
          case 'OBJECT_ATTRIBUTES' :
            this.objectAttributeChanges(columnPrevVal, columnCurrVal, element);
            break;
          case 'CLASSIFICATION' :
            this.classificationChanges(columnPrevVal, columnCurrVal, element);
            break;
          case 'SCHEMA' :
            this.schemaChanges(columnPrevVal, columnCurrVal, element);
            break;
          case 'OWNERSHIP' :
            this.ownershipChanges(columnPrevVal, columnCurrVal, element);
            break;
          default: this.displayChangeLogs();
        }
        this.changeLogs.push(element);
      });
      this.eventCount.emit(this.changeLogs.length);
    }, error => {
      this.changeLogs = [];
    }, () => {
      this.displayChangeLogs();
    });
  }
  datasetNameChanges (columnPrevVal, columnCurrVal, element) {
    // Dataset previous Name
    element.columnPrevVal = columnPrevVal.value === undefined ? 'N/A' : columnPrevVal.value;
    // Dataset current Name
    element.columnCurrVal = columnCurrVal.value;
    element.timelineDimension = 'Dataset';
    element.heading = 'Dataset name changes';
    element.username = columnCurrVal.username;
    this.getUserImage(element);
    // Display Dataset dimension changes by color given in Enum
    for ( const color in this.timelineEnumMap) {
          if ( this.timelineEnumMap[color][0] === 'DATASET') {
            element.givecolor = this.timelineEnumMap[color][2];
          }
    }
  }
  datasetDescriptionChanges(columnPrevVal, columnCurrVal, element) {
    element.columnPrevVal = columnPrevVal;
    element.columnCurrVal = columnCurrVal;
    element.timelineDimension = 'Dataset Description';
    element.username = columnCurrVal.username;
    this.getUserImage(element);
    element.heading = 'Dataset Description Changes';
    for ( const color in this.timelineEnumMap) {
      if ( this.timelineEnumMap[color][0] === 'OBJECT_DESC') {
        element.givecolor = this.timelineEnumMap[color][2];
      }
    }
  }
  tagChanges(columnPrevVal, columnCurrVal, element) {
    const currTagList = [];
    // Get the current Tags list with new added Tag
    for (let j = 0; j < columnCurrVal.length; j++) {
      const currTag = columnCurrVal[j];
      currTagList.push(currTag.value);
      element.username = currTag.username;
      this.getUserImage(element);
    }
    element.columnPrevVal = 'N/A';
    const prevTagList = [];
    // Get the previous Tags list
    for (let i = 0; i < columnPrevVal.length; i++) {
      const tags = columnPrevVal[i];
      prevTagList.push(tags.value);
    }
    if (prevTagList.length > 0) {
      element.columnPrevVal = prevTagList;
    } else {
      element.addedTag = currTagList;
    }
    // Get the newly added tag to show on UI with different color
    for ( let currTag = 0; currTag < currTagList.length; currTag++) {
      for (let prevTag = 0; prevTag < prevTagList.length ; prevTag++) {
        if (currTagList[currTag] !== prevTagList[prevTag]) {
          element.addedTag = currTagList[currTag];
        }
      }
    }
    element.timelineDimension = 'Tag';
    element.heading = 'Tag Changes';
    element.addedTagColor = 'green';
    for ( const color in this.timelineEnumMap) {
      if ( this.timelineEnumMap[color][0] === 'TAG') {
        element.givecolor = this.timelineEnumMap[color][2];
      }
    }
  }
  datasetColumnDescChanges(columnPrevVal, columnCurrVal, element) {
    element.heading = 'Dataset Column Description changes';
    element.columnPrevVal = columnPrevVal;
    element.columnCurrVal = columnCurrVal;
    element.username = columnCurrVal.username;
    this.getUserImage(element);
    element.timelineDimension = 'Dataset Column Description';
    for ( const color in this.timelineEnumMap) {
      if ( this.timelineEnumMap[color][0] === 'COLUMN_DESC') {
        element.givecolor = this.timelineEnumMap[color][2];
      }
    }
  }
  objectAttributeChanges(columnPrevVal, columnCurrVal, element) {
    const objectCurrList = [];
    const objectPrevList = [];
    objectCurrList.push(columnCurrVal);
    objectPrevList.push(columnPrevVal);
    if (objectPrevList !== undefined) {
      element.columnPrevVal = objectPrevList;
    } else {
      objectPrevList.push('N/A');
      element.columnPrevVal = objectPrevList;
    }
    element.columnCurrVal = objectCurrList;
    element.columnPrevVal = objectPrevList;
    element.username = columnCurrVal.username;
    this.getUserImage(element);
    element.timelineDimension = 'Object Attributes';
    element.heading = 'Object Attribute changes'
    for ( const color in this.timelineEnumMap) {
      if ( this.timelineEnumMap[color][0] === 'OBJECT_ATTRIBUTES') {
        element.givecolor = this.timelineEnumMap[color][2];
      }
    }
  }
  ownershipChanges(columnPrevVal, columnCurrVal, element) {
    element.columnPrevVal = 'N/A';
    element.timelineDimension = 'Ownership';
    element.heading = 'Ownership Changes';

    const currOwnerList = [];
    // Get the current Owners list with newly added owner
    for (let j = 0; j < columnCurrVal.length; j++) {
      const currOwner = columnCurrVal[j];
      currOwnerList.push(currOwner.value);
      element.username = currOwner.username;
    }
    const prevOwnerList = [];
    // Get the previous Users list
    for (let i = 0; i < columnPrevVal.length; i++) {
      const owners = columnPrevVal[i];
      prevOwnerList.push(owners.value);
    }
    if (prevOwnerList.length > 0) {
      element.columnPrevVal = prevOwnerList;
    } else {
      element.addedTag  = currOwnerList;
    }
    // Get the prev and curr ownersList if owner is deleted
    if (prevOwnerList.length > currOwnerList.length) {
      element.status = 'Deleted';
      const deletedOwnerList = [];
      for (let prevOwner = 0; prevOwner < prevOwnerList.length; prevOwner++) {
        if (!(currOwnerList.includes(prevOwnerList[prevOwner]))) {
          deletedOwnerList.push(prevOwnerList[prevOwner]);
        }
      }
      element.addedTag = deletedOwnerList;
      element.columnPrevVal = prevOwnerList;
      element.columnCurrVal = currOwnerList;
    } else if (prevOwnerList.length < currOwnerList.length) {           // Get the prev and curr ownersList if owner is added
      for (let currOwner = 0; currOwner < currOwnerList.length; currOwner++) {
        for (let prevOwner = 0; prevOwner < prevOwnerList.length; prevOwner++) {
          if (prevOwnerList[currOwner] !== prevOwnerList[prevOwner]) {
            element.addedTag = currOwnerList[currOwner];
          }
        }
      }
      element.status = 'Added';
    } else {
      if (columnPrevVal.value.includes('@')) { // Get the prev and curr ownersEmailList if ownerEmail is added

        element.columnPrevVal = columnPrevVal.value.split(',');
        element.columnCurrVal = columnCurrVal.value.split(',');
        element.ownerName = columnCurrVal.ownerName;
        if (element.columnPrevVal.length < element.columnCurrVal.length) {
          element.status = 'EA';
          const emailOwnerList = [];
          for (let currOwner = 0; currOwner < element.columnCurrVal.length; currOwner++) {
            if (!(element.columnPrevVal.includes(element.columnCurrVal[currOwner]))) {
              emailOwnerList.push(element.columnCurrVal[currOwner]);
            }
          }
          element.addedOwner = emailOwnerList;
          element.columnCurrVal = columnCurrVal;
        }
        if (element.columnPrevVal.length > element.columnCurrVal.length) { // Get the prev and curr ownersEmailList if ownerEmail is deleted
          element.status = 'ED';
          const deletedEmailOwnerList = [];
          for (let prevOwner = 0; prevOwner < element.columnPrevVal.length; prevOwner++) {
            if (!(element.columnCurrVal.includes(element.columnPrevVal[prevOwner]))) {
              deletedEmailOwnerList.push(element.columnPrevVal[prevOwner]);
            }
          }
          element.deletedOwner = deletedEmailOwnerList;
          element.columnPrevVal = element.columnPrevVal;
          element.columnCurrVal = element.columnCurrVal;
        }
      } else { // Get the prev and curr ownershipComment if Description is modified
        element.columnPrevVal = columnPrevVal;
        element.columnCurrVal = columnCurrVal;
        element.status = 'Modified';
      }
      element.username = columnCurrVal.username;
    }
    for (const color in this.timelineEnumMap) {
      if (this.timelineEnumMap[color][0] === 'OWNERSHIP') {
        element.givecolor = this.timelineEnumMap[color][2];
      }
    }
  }

  classificationChanges(columnPrevVal, columnCurrVal, element) {
    const classificationCurrList = [];
    const classificationPrevList = [];
    classificationCurrList.push(columnCurrVal);
    classificationPrevList.push(columnPrevVal);
    element.username = columnCurrVal.userName;
    this.getUserImage(element);
    element.timelineDimension = 'Classification';
    element.heading = 'Classification changes';
    element.columnPrevVal = columnPrevVal;
    element.columnCurrVal = classificationCurrList;
    if (element.storageDatasetChangeType === 'C') {
      element.status = 'Added';
    } else {
      element.status = 'Modified';
    }
    for ( const color in this.timelineEnumMap) {
      if ( this.timelineEnumMap[color][0] === 'CLASSIFICATION') {
        element.givecolor = this.timelineEnumMap[color][2];
      }
    }
  }
  schemaChanges(columnPrevVal, columnCurrVal, element) {
    const preVal: Array<any> = columnPrevVal ? columnPrevVal.value : [];
    const curVal: Array<any> = columnCurrVal ? columnCurrVal.value : [];
    const currMap = {};
    const prevMap = {};
    // construct currMap, prevMap HashMaps with columnName and its current value as key value pair.
      curVal.forEach(val => currMap[val.columnName] = val);
      preVal.forEach(val => prevMap[val.columnName] = val);

    element.username = columnCurrVal.userName;
    this.getUserImage(element);
    element.timelineDimension = 'Schema';
    element.heading = 'Schema changes';
    for ( const color in this.timelineEnumMap) {
      if ( this.timelineEnumMap[color][0] === 'SCHEMA') {
        element.givecolor = this.timelineEnumMap[color][2];
      }
    }
    const addSchemaList = [];
    const deleteSchemaList = [];
    // first we will check to see if there is any schema, else add all the rows
    if (preVal === undefined) {
      for (const key in currMap) {
        const curValue = currMap[key];
        addSchemaList.push(curValue);
        element.columnCurrVal = addSchemaList;
      }
      element.status = 'Added';
    } else if (curVal === undefined) {  // if previous schema value i.e particular row does not exits in current schema , delete the rows
      for (const key in prevMap) {
        const prevValue = prevMap[key];
        addSchemaList.push(prevValue);
        element.columnCurrVal = deleteSchemaList;
      }
      element.status = 'Deleted';
    } else {
      const finalModifyList = [];
      for (const key in currMap) {
        const curValue = currMap[key];
        const preValue = prevMap[key];
        if (preValue) {
          if (JSON.stringify(preValue) === JSON.stringify(curValue)) {
            // previous and current value are same for given row
          } else {
            const schemaList = [];
            const curList = [];
            const prevList = [];
            const result = [];
            const modifiedType = [];
            const modifiedTypeList = [];
            prevList.push(preValue);
            curList.push(curValue);
            for (let a = 0; a < prevList.length; a++) {
              for (let b = 0; b < curList.length; b++) {
                if (prevList[a].columnType === curList[b].columnType && prevList[a].columnClass === curList[b].columnClass && prevList[a].partitionStatus === curList[b].partitionStatus) {
                } else {
                  let columnType = '';
                  let columnClass = '';
                  let partitionStatus = '';
                  columnType =  curList[b].columnType;
                  columnClass = curList[b].columnClass;
                  partitionStatus = curList[b].partitionStatus;
                  const columnName = curList[b].columnName;
                  result.push(columnName);
                  result.push(columnType);
                  result.push(columnClass);
                  result.push(partitionStatus);
                  // push the changed rows details in FinalModifyList
                  finalModifyList.push(result);
                  modifiedTypeList.push(modifiedType);
                }
              }
            }
            // schemaList gives the full updated schema list
            const currSchemaMap = {};
            curVal.forEach(val => currSchemaMap[val.columnName] = val);
            for (const schema in currSchemaMap) {
              const currValue = currSchemaMap[schema];
              schemaList.push(currValue);
            }
            // Remove the rows from schemaList having no changes to show only modified Schema rows on UI
            for (let k = 0; k < schemaList.length; k++) {
              for (let x = 0; x < finalModifyList.length; x++) {
                if (schemaList[k].columnName === finalModifyList[x][0]) {
                  schemaList.splice(k, 1);
                }
              }
            }
            element.changeColumn = modifiedTypeList;
            element.changeValue = finalModifyList;
            element.columnCurrVal = schemaList;
            element.status = 'Modified';      // value change for particular column
          }
        } else {
          element.columnCurrVal = curValue;
          const curList = [];
          const modifiedList = [];
          const schemaList = [];
          curList.push(curValue);
          const schemaMap = {};
          curVal.forEach(val => schemaMap[val.columnName] = val);
          for (const schema in schemaMap) {
            const currValue = schemaMap[schema];
            schemaList.push(currValue);
          }
          for (const schema in schemaMap) {
            const perValue = prevMap[schema];
            if (!perValue) {
              const addition = schemaMap[schema];
              modifiedList.push(addition);
            }
          }
          for (let k = 0; k < schemaList.length; k++) {
            for (let no = 0; no < modifiedList.length; no++) {
              if (schemaList[k].columnName === modifiedList[no].columnName) {
                schemaList.splice(k, 1);
              }
            }
          }
          element.changeValue = modifiedList;
          element.columnCurrVal = schemaList;
          element.status = 'Added';         // Row added in present schema
        }
      }
      for (const key in prevMap) {
        const curValue = currMap[key];
        const deletedList = [];
        if (!curValue) {
          const prevValue = prevMap[key];
          addSchemaList.push(prevValue);
          for (const key1 in currMap) {
            const currValue = currMap[key1];
            deletedList.push(currValue);
          }
          element.changeValue = addSchemaList;
          element.columnCurrVal = deletedList;
          element.status = 'Deleted';  // Row deleted in present schema
        }
      }
    }
  }

  displayChangeLogs() {
    this.filterList = [];
    let description = '';
    for (let log = 0; log < this.changeLogs.length; log++) {
      // timelineDimension can be 'DATASET', 'TAG' etc.
      const timelineDimension = this.changeLogs[log].changeColumnType;
      for ( const key in this.timelineEnumMap) {
        // get the Flag from user selected timelineDimension. eg. get 'DATASET' flag from user selected input Dataset name.
        const input = this.timelineEnumMap[key][0];
        if (timelineDimension === input) {
          description = this.timelineEnumMap[key][1];
        }
      }
      if (this.filtersCtrl.value.toString().includes('ALL')) { // Show all the changeLogs when page is loaded. (By default All is selected)
        this.flag = false;
        this.changeLogs = this.changeLogs.sort((a, b): number => {
          const latestDate = new Date(a.updatedTimestamp);
          const oldDate = new Date(b.updatedTimestamp);
          return latestDate < oldDate ? 1 : -1;
        });
      } else if (this.filtersCtrl.value.toString().includes(description)) { // Filter the changeLog response with User given input
          this.filterList.push(this.changeLogs[log]);
          this.filterList = this.filterList.sort((a, b): number => {
            const latestDate = new Date(a.updatedTimestamp);
            const oldDate = new Date(b.updatedTimestamp);
            return latestDate < oldDate ? 1 : -1;
          });
        } else {
        // if flag is true, there are no changeLogs present for given timelineDimension
          this.flag = true;
        }

    }
  }
  getUserImage(element) {
    this.catalogService.getUserByName(element.username)
      .subscribe(data => {
        if (data) {
          this.user = data;
          element.imageUrl = 'https://image.shutterstock.com/image-illustration/creative-illustration-default-avatar-profile-260nw-1400808113.jpg' ;
        }
      }, error => {
        element.imageUrl = 'https://image.shutterstock.com/image-illustration/creative-illustration-default-avatar-profile-260nw-1400808113.jpg' ;
      });
  }
  onTimelineDimensionChange() {
    this.displayChangeLogs();
  }

  protected filterTimelineDimensions() {
    if (!this.displayList) {
      return;
    }
    let search = this.timelineFilterCtrl.value;
    if (!search) {
      this.filteredTimeline.next(this.displayList.slice());
      return;
    } else {
      search = search.toLowerCase();
    }
    this.filteredTimeline.next(this.displayList.filter(a => a.toLowerCase().indexOf(search) > -1));
  }

}
