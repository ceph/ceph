import { Component, Input, ViewEncapsulation } from '@angular/core';

import { OsdIoOverviewModel } from '~/app/shared/models/osd.model';

@Component({
  selector: 'cd-osd-io-card',
  templateUrl: './osd-io-card.component.html',
  styleUrls: ['./osd-io-card.component.scss'],
  encapsulation: ViewEncapsulation.None,
  standalone: false
})
export class OsdIoCardComponent {
  @Input() overviewModel: OsdIoOverviewModel;
}
