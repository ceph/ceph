import { Component, Input, OnChanges } from '@angular/core';

import * as _ from 'lodash';

import { CdTableSelection } from '../../../../shared/models/cd-table-selection';

@Component({
  selector: 'cd-configuration-details',
  templateUrl: './configuration-details.component.html',
  styleUrls: ['./configuration-details.component.scss']
})
export class ConfigurationDetailsComponent implements OnChanges {
  @Input()
  selection: CdTableSelection;
  selectedItem: any;
  flags = {
    runtime: 'The value can be updated at runtime.',
    no_mon_update:
      'Daemons/clients do not pull this value from the monitor config database. ' +
      `We disallow setting this option via 'ceph config set ...'. This option should be ` +
      'configured via ceph.conf or via the command line.',
    startup: 'Option takes effect only during daemon startup.',
    cluster_create: 'Option only affects cluster creation.',
    create: 'Option only affects daemon creation.'
  };

  constructor() {}

  ngOnChanges() {
    if (this.selection.hasSelection) {
      this.selectedItem = this.selection.first();
      this.selectedItem.services = _.split(this.selectedItem.services, ',');
    }
  }
}
