import {Component, Input, OnInit} from '@angular/core';

import {CatalogService} from '../../services/catalog.service';

@Component({
  selector: 'app-catalog-object-detail',
  templateUrl: './catalog-object-detail.component.html',
  styleUrls: ['./catalog-object-detail.component.scss'],
})

export class CatalogObjectDetailComponent implements OnInit {
  @Input() object: string;
  @Input() project: string;
  public detailsLoading = false;
  public statusData = {};
  public columnList = [];
  public objectAttributesList = [];
  public isSelfDiscovered;

  constructor(private catalogService: CatalogService) {
  }

  ngOnInit() {
    this.getObjectDetails();
  }

  getObjectDetails() {
    this.detailsLoading = true;
    this.catalogService.getObjectDetails(this.object)
      .subscribe(data => {
        this.statusData = data;
        if (data['isSelfDiscovered'] === 'Y') {
          this.isSelfDiscovered = 'No';
        } else {
          this.isSelfDiscovered = 'Yes';
        }
        this.columnList = data.objectSchema;
        this.objectAttributesList = data.objectAttributes;
        this.detailsLoading = false;
      }, error => {
        this.statusData = {};
        this.detailsLoading = false;
      });
  }

}
