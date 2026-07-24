import { Component, Input, ViewEncapsulation } from '@angular/core';
import { PoolOverviewModel } from '~/app/shared/models/pool-overview.model';

@Component({
  selector: 'cd-pool-io-card',
  templateUrl: './pool-io-card.component.html',
  styleUrls: ['./pool-io-card.component.scss'],
  encapsulation: ViewEncapsulation.None,
  standalone: false
})
export class PoolIoCardComponent {
  @Input() overviewModel: PoolOverviewModel;
}
