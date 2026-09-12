import { Component, Input, ViewEncapsulation } from '@angular/core';

import { OsdCapacityOverviewModel } from '~/app/shared/models/osd.model';

@Component({
  selector: 'cd-osd-capacity-card',
  templateUrl: './osd-capacity-card.component.html',
  styleUrls: ['./osd-capacity-card.component.scss'],
  encapsulation: ViewEncapsulation.None,
  standalone: false
})
export class OsdCapacityCardComponent {
  @Input() overviewModel: OsdCapacityOverviewModel;
}
