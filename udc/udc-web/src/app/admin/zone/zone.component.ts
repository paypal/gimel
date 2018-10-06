import {Component, OnInit} from '@angular/core';
import {CatalogService} from '../../udc/catalog/services/catalog.service';
import {Zone} from '../models/catalog-zone';

@Component({
  selector: 'app-udc-admin-zone',
  templateUrl: './zone.component.html',
  styleUrls: ['./zone.component.scss'],
})
export class ZoneComponent {
  projectList = [];
  displayList = [];
  loaded = true;
  projectName: string;
  projectLoaded = false;
  refresh = false;
  defaultZone: Zone = new Zone();

  constructor(private catalogService: CatalogService) {
  }

  ngOnInit() {
    this.projectLoaded = false;

    this.defaultZone.zoneId = 0;
    this.defaultZone.zoneName = 'All';
    this.defaultZone.zoneDescription = 'All';
    this.projectList.push(this.defaultZone);
    this.catalogService.getZonesList().subscribe(data => {
      data.map(element => {
        this.projectList.push(element);
      });
    }, error => {
      this.displayList = this.projectList = [];
      this.projectLoaded = true;
    }, () => {
      this.displayList = this.projectList.sort((a, b): number => {
        return a.zoneName > b.zoneName ? 1 : -1;
      });
      this.projectName = this.defaultZone.zoneName;
      this.projectLoaded = true;
    });
  }

  disableDropDown() {
    // this.projectLoaded = false;
    this.loaded = false;
  }

  enableDropDown() {
    this.loaded = true;
  }

  refreshProject() {
    this.refresh = !this.refresh;
    this.loaded = false;
  }

}
