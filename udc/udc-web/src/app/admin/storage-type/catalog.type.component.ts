import {Component, OnInit} from '@angular/core';
import {StorageSystem} from '../../udc/catalog/models/catalog-storagesystem';
import {CatalogService} from '../../udc/catalog/services/catalog.service';
import {Category} from '../models/catalog-category';


@Component({
  selector: 'app-catalog-home',
  templateUrl: './catalog.type.component.html',
  styleUrls: ['./catalog.type.component.scss'],
})

export class TypeComponent implements OnInit {
  projectList = [];
  displayList = [];
  loaded = true;
  projectName: string;
  projectLoaded = false;
  refresh = false;
  defaultSystem: Category = new Category();

  constructor(private catalogService: CatalogService) {
  }

  ngOnInit() {
    this.projectLoaded = false;

    this.defaultSystem.storageId = 0;
    this.defaultSystem.storageName = 'All';
    this.defaultSystem.storageDescription = 'All';
    this.projectList.push(this.defaultSystem);
    this.catalogService.getStorageCategories().subscribe(data => {
      data.map(element => {
        this.projectList.push(element);
      });
    }, error => {
      this.displayList = this.projectList = [];
      this.projectLoaded = true;
    }, () => {
      this.displayList = this.projectList.sort((a, b): number => {
        return a.storageName > b.storageName ? 1 : -1;
      });
      // this.projectName = this.displayList[0]['storageSystemName'];
      this.projectName = this.defaultSystem.storageName;
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
