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

import {Component, OnInit} from '@angular/core';
import {FormGroup, FormBuilder} from '@angular/forms';
import {MatDialogRef} from '@angular/material';
import {onValueChanged} from '../../../shared/utils';
import {DatePipe} from '@angular/common';

@Component({
  selector: 'app-catalog-system-discovery-metrics-dialog', templateUrl: './catalog-system-discovery-metrics-dialog.component.html', styleUrls: ['./catalog-system-discovery-metrics-dialog.component.scss'],
})

export class CatalogSystemDiscoveryMetricsDialogComponent implements OnInit {
  heading = '';
  viewTypeForm: FormGroup;
  storageSystemId: number;
  storageSystemName: string;
  lastInvocation: string;
  nextInvocation: string;
  totalInserts: number;
  totalDeletes: number;
  totalUpdates: number;
  runStatus: string;
  public columnChartData: any;
  nextMilliSeconds: number;
  discoverySla: string;
  dateFormat = 'yyyy-MM-dd HH:mm:ss';

  constructor(public dialogRef: MatDialogRef<CatalogSystemDiscoveryMetricsDialogComponent>, private fb: FormBuilder, private datePipe: DatePipe) {

  }

  ngOnInit() {
    this.viewTypeForm = this.fb.group({});
    this.heading = 'Discovery Metrics for ' + this.storageSystemName;
    this.columnChartData = {
      chartType: 'ColumnChart', dataTable: [['Type', 'Count'], ['Datasets Discovered', this.totalInserts], ['Datasets Updated', this.totalUpdates], ['Datasets Deleted', this.totalDeletes]], options: {title: 'Updates from the last run'},
    };
    if (this.lastInvocation && this.lastInvocation.length > 0) {
      const lastInvokedDate = new Date(this.lastInvocation);
      this.nextMilliSeconds = lastInvokedDate.getTime() + (Number(this.discoverySla) * 60000);
      this.nextInvocation = this.datePipe.transform(new Date(this.nextMilliSeconds), this.dateFormat);
    }
    this.viewTypeForm.valueChanges.subscribe(data => onValueChanged(this.viewTypeForm, {}, []));
  }

  cancel() {
    this.dialogRef.close();
  }
}
