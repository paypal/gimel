import {Component, EventEmitter, Input, Output} from '@angular/core';
import {animate, style, transition, trigger} from '@angular/core';
import {ConfigService} from '../../core/services';

@Component({
  selector: 'app-sidenav-content', templateUrl: './sidenav-content.component.html', styleUrls: ['./sidenav-content.component.scss'], animations: [trigger('zoomFromLeftAnimation', [transition(':enter', [style({
    transform: 'scale(0.8) translate3d(-40px,0,0)', opacity: '0'
  }), animate('0.5s ease-in-out', style({transform: 'scale(1)', opacity: '1'})),]),])],
})
export class SidenavContentComponent {
  @Input('tabs') tabs: { label: string, link: string };
  @Input() opened = true;
  @Output() close = new EventEmitter();
}
