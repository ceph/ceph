import { Component, Input, ViewEncapsulation } from '@angular/core';
import { PoolOverviewModel } from '~/app/shared/models/pool-overview.model';

@Component({
  selector: 'cd-pool-capacity-protection-card',
  templateUrl: './pool-capacity-protection-card.component.html',
  styleUrls: ['./pool-capacity-protection-card.component.scss'],
  encapsulation: ViewEncapsulation.None,
  standalone: false
})
export class PoolCapacityProtectionCardComponent {
  @Input() overviewModel: PoolOverviewModel;
}
