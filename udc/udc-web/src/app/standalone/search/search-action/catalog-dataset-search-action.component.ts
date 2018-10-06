import {Component, Input, Output, EventEmitter} from '@angular/core';
import {
  MdDialogConfig,
} from '@angular/material';
import {ObjectSchemaMap} from '../../../udc/catalog/models/catalog-objectschema';
import {CatalogService} from '../../../udc/catalog/services/catalog.service';

@Component({
  selector: 'app-catalog-search-dataset-action', templateUrl: './catalog-dataset-search-action.component.html', styleUrls: ['./catalog-dataset-search-action.component.scss'],
})

export class CatalogDatasetSearchActionComponent {
  @Input() project: string;
  @Input() storageDataSetId: number;
  @Input() storageDataSetName: string;
  @Input() storageDataSetAliasName: string;
  @Input() storageDataSetDescription: string;
  @Input() createdUser: string;
  @Input() objectId: number;
  @Input() public errorStatus: boolean;
  @Input() isGimelCompatible: string;
  @Input() isReadCompatible: string;
  @Input() zoneName: string;
  @Input() isAccessControlled: string;
  public inProgress = false;
  public actionMsg: string;
  object: ObjectSchemaMap;
  dialogConfig: MdDialogConfig = {width: '1000px', height: '90vh'};
  sampleDataURL: string;

  @Output() refresh: EventEmitter<string> = new EventEmitter();

  constructor(private catalogService: CatalogService) {
    this.sampleDataURL = catalogService.serverWithPort + '/' + 'standalone/sampledata/' + this.storageDataSetName + '/' + this.objectId;
  }
}
