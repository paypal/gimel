import {
  Component,
  ChangeDetectionStrategy,
  EventEmitter,
  Input,
  Output,
 } from '@angular/core';
import {CummulativeDataset} from '../../../udc/catalog/models/catalog-cumulative-dataset';

@Component({
  selector: 'app-gh-searchbox-result',
  templateUrl: './gh-searchbox-result.component.html',
  styleUrls: ['./gh-searchbox-result.component.scss'],
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class GhSearchboxResultComponent {
  @Input() cds: CummulativeDataset[];
  @Input() searchTerm: string;
  @Output() onClick = new EventEmitter<void>();

  closeSearch() {
    this.onClick.emit();
  }
}
