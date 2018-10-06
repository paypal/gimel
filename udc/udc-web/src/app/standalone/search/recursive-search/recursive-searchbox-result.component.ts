import {
  Component,
  ChangeDetectionStrategy,
  EventEmitter,
  Input,
  Output,
 } from '@angular/core';
import {CummulativeDataset} from '../../../udc/catalog/models/catalog-cumulative-dataset';

@Component({
  selector: 'app-recursive-searchbox-result',
  templateUrl: './recursive-searchbox-result.component.html',
  styleUrls: ['./recursive-searchbox-result.component.scss'],
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class RecursiveSearchboxResultComponent {
  @Input() cds: CummulativeDataset[];
  @Input() searchTerm: string;
  @Output() onClick = new EventEmitter<void>();

  closeSearch() {
    this.onClick.emit();
  }
}
