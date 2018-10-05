import {Component, OnInit} from '@angular/core';
import {FormGroup, FormBuilder} from '@angular/forms';
import {MdDialogRef} from '@angular/material';
import {TeradataPolicy} from '../../models/catalog-teradata-policy';
import {DerivedPolicy} from '../../models/catalog-derived-policy';

@Component({
  selector: 'app-catalog-access-dialog', templateUrl: './catalog-dataset-access-dialog.component.html', styleUrls: ['./catalog-dataset-access-dialog.component.scss'],
})

export class CatalogDatasetAccessDialogComponent implements OnInit {
  heading = '';
  policyForm: FormGroup;
  public datasetName: string;
  public accessControlList = [];
  public typeName: string;

  constructor(public dialogRef: MdDialogRef<CatalogDatasetAccessDialogComponent>, private fb: FormBuilder) {
  }

  ngOnInit() {
    this.policyForm = this.fb.group({});
    this.heading = 'Access Control Policies for ' + this.datasetName;
  }

  cancel() {
    this.dialogRef.close();
  }
}
