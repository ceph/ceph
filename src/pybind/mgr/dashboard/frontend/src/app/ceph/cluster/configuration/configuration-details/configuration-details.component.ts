import { Component, Input, OnChanges } from '@angular/core';

import { I18n } from '@ngx-translate/i18n-polyfill';
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
    runtime: this.i18n('The value can be updated at runtime.'),
    no_mon_update: this.i18n(`Daemons/clients do not pull this value from the
      monitor config database. We disallow setting this option via 'ceph config
      set ...'. This option should be configured via ceph.conf or via the
      command line.`),
    startup: this.i18n('Option takes effect only during daemon startup.'),
    cluster_create: this.i18n('Option only affects cluster creation.'),
    create: this.i18n('Option only affects daemon creation.')
  };

  constructor(private i18n: I18n) {}

  ngOnChanges() {
    if (this.selection.hasSelection) {
      this.selectedItem = this.selection.first();
      this.selectedItem.services = _.split(this.selectedItem.services, ',');
    }
  }
}
