import {Component, OnInit} from '@angular/core';
import {CatalogService} from '../../udc/catalog/services/catalog.service';
import {Type} from '../models/catalog-type';


@Component({
  selector: 'app-udc-admin-system',
  templateUrl: './catalog.system.component.html',
  styleUrls: ['./catalog.system.component.scss'],
})

export class SystemComponent implements OnInit {
  projectList = [];
  displayList = [];
  loaded = true;
  projectName: string;
  projectLoaded = false;
  refresh = false;
  defaultType: Type = new Type();

  constructor(private catalogService: CatalogService) {
  }

  ngOnInit() {
    this.projectLoaded = false;

    this.defaultType.storageTypeId = 0;
    this.defaultType.storageTypeName = 'All';
    this.defaultType.storageTypeDescription = 'All';
    this.projectList.push(this.defaultType);
    this.catalogService.getStorageTypes().subscribe(data => {
      data.map(element => {
        this.projectList.push(element);
      });
    }, error => {
      this.displayList = this.projectList = [];
      this.projectLoaded = true;
    }, () => {
      this.displayList = this.projectList.sort((a, b): number => {
        return a.storageTypeName > b.storageTypeName ? 1 : -1;
      });
      this.projectName = this.defaultType.storageTypeName;
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
