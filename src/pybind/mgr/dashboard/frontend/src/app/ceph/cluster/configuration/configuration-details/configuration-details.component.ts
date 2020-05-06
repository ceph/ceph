import { Component, Input, OnChanges } from '@angular/core';

import * as _ from 'lodash';

@Component({
  selector: 'cd-configuration-details',
  templateUrl: './configuration-details.component.html',
  styleUrls: ['./configuration-details.component.scss']
})
export class ConfigurationDetailsComponent implements OnChanges {
  @Input()
  selection: any;
  flags = {
    runtime: $localize`The value can be updated at runtime.`,
    no_mon_update: $localize`Daemons/clients do not pull this value from the
      monitor config database. We disallow setting this option via 'ceph config
      set ...'. This option should be configured via ceph.conf or via the
      command line.`,
    startup: $localize`Option takes effect only during daemon startup.`,
    cluster_create: $localize`Option only affects cluster creation.`,
    create: $localize`Option only affects daemon creation.`
  };

  ngOnChanges() {
    if (this.selection) {
      this.selection.services = _.split(this.selection.services, ',');
    }
  }
}
