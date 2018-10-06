import {Component, OnInit} from '@angular/core';
import {ActivatedRoute} from '@angular/router';
import {Observable} from 'rxjs/Observable';
import {fadeInOutAnimation} from '../../shared/animations/animations';
import {CatalogService} from '../../udc/catalog/services/catalog.service';

@Component({
  selector: 'app-ghcontexthelp',
  templateUrl: './ghcontexthelp.component.html',
  styleUrls: ['./ghcontexthelp.component.scss'],
  animations: [fadeInOutAnimation],
})
export class GhcontexthelpComponent implements OnInit {
  routeData$: Observable<any>;
  clusters = [];
  clusterList = [];
  categories = [];
  categoryList = [];
  types = [];
  typeList = [];
  systems = [];
  systemList = [];

  constructor(private activatedRoute: ActivatedRoute, private catalogService: CatalogService) {
  }

  ngOnInit() {
    let route = this.activatedRoute;
    this.catalogService.getClusterList().subscribe(data => {
      data.map(element => {
        this.clusters.push(element);
      });
    }, error => {
      this.clusterList = this.clusters = [];
    }, () => {
      this.clusterList = this.clusters.sort((a, b): number => {
        return a.clusterName > b.clusterName ? 1 : -1;
      });
    });

    this.catalogService.getStorageCategories().subscribe(data => {
      data.map(element => {
        this.categories.push(element);
      });
    }, error => {
      this.categoryList = this.categories = [];
    }, () => {
      this.categoryList = this.categories.sort((a, b): number => {
        return a.storageName > b.storageName ? 1 : -1;
      });
    });

    this.catalogService.getStorageTypes().subscribe(data => {
      data.map(element => {
        this.types.push(element);
      });
    }, error => {
      this.typeList = this.types = [];
    }, () => {
      this.typeList = this.types.sort((a, b): number => {
        return a.storageTypeName > b.storageTypeName ? 1 : -1;
      });
    });

    this.catalogService.getStorageSystems().subscribe(data => {
      data.map(element => {
        this.systems.push(element);
      });
    }, error => {
      this.systemList = this.systems = [];
    }, () => {
      this.systemList = this.systems.sort((a, b): number => {
        return a.storageSystemName > b.storageSystemName ? 1 : -1;
      });
    });
    while (route.firstChild) {
      route = route.firstChild;
    }
    this.routeData$ = route.data;
  }
}
