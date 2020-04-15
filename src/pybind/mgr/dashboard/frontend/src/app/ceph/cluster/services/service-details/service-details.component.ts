import { Component, Input, OnInit, ViewChild } from '@angular/core';
import { TabsetComponent } from 'ngx-bootstrap/tabs';

import { CdTableSelection } from '../../../../shared/models/cd-table-selection';
import { Permissions } from '../../../../shared/models/permissions';

@Component({
  selector: 'cd-service-details',
  templateUrl: './service-details.component.html',
  styleUrls: ['./service-details.component.scss']
})
export class ServiceDetailsComponent implements OnInit {
  @ViewChild(TabsetComponent, { static: false })
  tabsetChild: TabsetComponent;

  @Input()
  permissions: Permissions;

  @Input()
  selection: CdTableSelection;

  constructor() {}

  ngOnInit() {}
}
