import { Component, Input, ViewChild } from '@angular/core';

import { TabsetComponent } from 'ngx-bootstrap/tabs';

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

  @ViewChild(TabsetComponent, { static: false })
  tabsetChild: TabsetComponent;

  get selectedHostname(): string {
    return this.selection !== undefined ? this.selection['hostname'] : null;
  }

  constructor() {}
}
