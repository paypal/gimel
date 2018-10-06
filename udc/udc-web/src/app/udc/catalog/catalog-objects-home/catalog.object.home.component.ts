import {Component, OnInit} from '@angular/core';

import {CatalogService} from '../services/catalog.service';
import {StorageSystem} from '../models/catalog-storagesystem';

@Component({
  selector: 'app-catalog-object-home',
  templateUrl: './catalog.object.home.component.html',
  styleUrls: ['./catalog.object.home.component.scss'],
})

export class CatalogObjectHomeComponent implements OnInit {
  projectContainerList = [];
  projectSystemList = [];
  displayContainerList = [];
  displaySystemList = [];
  sloaded = true;
  cloaded = true;
  systemName: string;
  containerName: string;
  systemsLoaded = false;
  containersLoaded = false;
  refreshContainer = false;
  defaultSystem: StorageSystem = new StorageSystem();
  defaultContainer: string;
  systemMap: Map<string, StorageSystem>;
  systemMapById: Map<number, StorageSystem>;
  constructor(private catalogService: CatalogService) {
    this.defaultSystem.storageSystemId = 0;
    this.defaultSystem.storageSystemName = 'All';
    this.defaultSystem.storageSystemDescription = 'All';
    this.defaultContainer = 'All';
    this.systemMap = new Map();
    this.systemMapById = new Map();
  }

  loadStorageSystems() {
    this.systemsLoaded = false;
    this.projectSystemList.push(this.defaultSystem);
    this.catalogService.getStorageSystems().subscribe(data => {
      data.map(element => {
        this.projectSystemList.push(element);
      });
    }, error => {
      this.displaySystemList = this.projectSystemList = [];
      this.systemsLoaded = true;
    }, () => {
      this.displaySystemList = this.projectSystemList.sort((a, b): number => {
        return a.storageSystemName > b.storageSystemName ? 1 : -1;
      });
      this.displaySystemList.forEach(system => {
        this.systemMap.set(system.storageSystemName, system);
        this.systemMapById.set(system.storageSystemId, system);
      });
      this.systemName = this.defaultSystem.storageSystemName;
      this.systemsLoaded = true;
    });
  }

  loadContainers() {
    this.containersLoaded = false;
    this.projectContainerList.push(this.defaultContainer);
    this.catalogService.getContainers().subscribe(data => {
      data.map(element => {
        this.projectContainerList.push(element);
      });
    }, error => {
      this.displayContainerList = this.projectContainerList = [];
      this.systemsLoaded = true;
    }, () => {
      this.displayContainerList = this.projectContainerList.sort((a, b): number => {
        return a > b ? 1 : -1;
      });
      this.enableDropDownSystem();
      this.containerName = this.defaultContainer;
      this.containersLoaded = true;
    });
  }

  onStorageSystemChange() {
    this.projectContainerList = [];
    this.projectContainerList.push(this.defaultContainer);
    this.containersLoaded = false;
    const system: StorageSystem = this.systemMap.get(this.systemName);
    this.catalogService.getContainersForSystem(system.storageSystemId.toString()).subscribe(data => {
      data.map(element => {
        this.projectContainerList.push(element);
      });
    }, error => {
      this.displayContainerList = this.projectContainerList = [];
      this.containersLoaded = true;
    }, () => {
      this.displayContainerList = this.projectContainerList.sort((a, b): number => {
        return a > b ? 1 : -1;
      });
      this.enableDropDownSystem();
      this.containerName = this.defaultContainer;
      this.containersLoaded = true;
    });
  }

  ngOnInit() {
    this.loadStorageSystems();
    this.loadContainers();
  }

  disableDropDownSystem() {
    this.sloaded = false;
  }

  disableDropDownContainer() {
    this.cloaded = false;
  }

  enableDropDownContainer() {
    this.cloaded = true;
  }

  refreshProject() {
    this.refreshContainer = !this.refreshContainer;
    this.cloaded = false;
  }

  enableDropDownSystem() {
    this.sloaded = true;
  }

}
