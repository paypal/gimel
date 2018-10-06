import {Component, OnInit} from '@angular/core';
import {FormGroup} from '@angular/forms';
import {StorageSystem} from '../../udc/catalog/models/catalog-storagesystem';
import {CatalogService} from '../../udc/catalog/services/catalog.service';

@Component({
  selector: 'app-udc-admin-category',
  templateUrl: './category.component.html',
  styleUrls: ['./category.component.scss'],
})
export class CategoryComponent {
  projectList = [];
  displayList = [];
  loaded = true;
  projectName: string;
  projectLoaded = false;
  refresh = false;
  defaultSystem: StorageSystem = new StorageSystem();

  constructor(private catalogService: CatalogService) {
  }

  ngOnInit() {
    this.projectLoaded = false;

    this.defaultSystem.storageSystemId = 0;
    this.defaultSystem.storageSystemName = 'All';
    this.defaultSystem.storageSystemDescription = 'All';
    this.projectList.push(this.defaultSystem);
    this.catalogService.getStorageSystems().subscribe(data => {
      data.map(element => {
        this.projectList.push(element);
      });
    }, error => {
      this.displayList = this.projectList = [];
      this.projectLoaded = true;
    }, () => {
      this.displayList = this.projectList.sort((a, b): number => {
        return a.storageSystemName > b.storageSystemName ? 1 : -1;
      });
      // this.projectName = this.displayList[0]['storageSystemName'];
      this.projectName = this.defaultSystem.storageSystemName;
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
