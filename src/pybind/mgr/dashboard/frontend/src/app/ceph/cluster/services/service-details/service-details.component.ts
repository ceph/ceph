import { Component, Input } from '@angular/core';

import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { Permissions } from '~/app/shared/models/permissions';

@Component({
  selector: 'cd-service-details',
  templateUrl: './service-details.component.html',
  styleUrls: ['./service-details.component.scss']
})
export class ServiceDetailsComponent {
  @Input()
  permissions: Permissions;

  @Input()
  selection: CdTableSelection;
}
