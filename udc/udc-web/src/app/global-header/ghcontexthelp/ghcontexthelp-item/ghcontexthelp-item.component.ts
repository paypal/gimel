import {
  Component,
  Input,
} from '@angular/core';

import { expandCollapseAnimation } from '../../../shared/animations/animations';

@Component({
  selector: 'app-ghcontexthelp-item',
  templateUrl: './ghcontexthelp-item.component.html',
  styleUrls: ['./ghcontexthelp-item.component.scss'],
  animations: [expandCollapseAnimation],
})
export class GhcontexthelpItemComponent {
  show: boolean;
  @Input() title;
  @Input() lastcontent = false;

  toggleHelp(event: Event) {
    event.preventDefault();
    event.stopPropagation();
    this.show = !this.show;
  }
}
