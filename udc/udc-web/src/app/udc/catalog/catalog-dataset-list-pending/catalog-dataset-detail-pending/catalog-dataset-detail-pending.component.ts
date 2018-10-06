import {Component, Input, OnInit} from '@angular/core';

import { CatalogService } from '../../services/catalog.service';

@Component({
  selector: 'app-catalog-dataset-pending-detail',
  templateUrl: './catalog-dataset-detail-pending.component.html',
  styleUrls: ['./catalog-dataset-detail-pending.component.scss'],
})

export class CatalogDatabaseDetailPendingComponent implements OnInit {
  @Input() dataset: string;
  @Input() project: string;
  public detailsLoading = false;
  public statusData = { };
  public columnList = [];
  public systemAttributesList = [];
  public objectAttributesList = [];
  public clusterNamesList = [];
  constructor(private catalogService: CatalogService) { }

  ngOnInit() {
    this.getDatasetDetails();
  }

  getDatasetDetails() {
    this.detailsLoading = true;
    this.catalogService.getDatasetPendingDetails(this.dataset)
      .subscribe(
        data => {
          this.statusData = data;
          this.detailsLoading = false;
          this.columnList = data.objectSchema;
          this.systemAttributesList = data.systemAttributes;
          this.objectAttributesList = data.objectAttributes;
          this.clusterNamesList = data.clusterNames;
        },
        error => {
          this.statusData = { };
          this.detailsLoading = false;
        }
    );
  }
}
