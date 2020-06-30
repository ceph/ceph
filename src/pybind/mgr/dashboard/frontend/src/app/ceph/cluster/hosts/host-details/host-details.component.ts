import { Component, Input } from '@angular/core';

import { Permissions } from '../../../../shared/models/permissions';

@Component({
  selector: 'cd-host-details',
  templateUrl: './host-details.component.html',
  styleUrls: ['./host-details.component.scss']
})
export class HostDetailsComponent {
  @Input()
  permissions: Permissions;

  @Input()
  selection: any;

  get selectedHostname(): string {
    return this.selection !== undefined ? this.selection['hostname'] : null;
  }
}
