import { Component, Input, OnChanges } from '@angular/core';

import * as _ from 'lodash';

import { CdTableSelection } from '../../../shared/models/cd-table-selection';
import { RgwDaemonService } from '../services/rgw-daemon.service';

@Component({
  selector: 'cd-rgw-daemon-details',
  templateUrl: './rgw-daemon-details.component.html',
  styleUrls: ['./rgw-daemon-details.component.scss']
})
export class RgwDaemonDetailsComponent implements OnChanges {
  metadata: any;
  serviceId = '';

  @Input() selection: CdTableSelection;

  constructor(private rgwDaemonService: RgwDaemonService) {}

  ngOnChanges() {
    // Get the service id of the first selected row.
    if (this.selection.hasSelection) {
      this.serviceId = this.selection.first().id;
    }
  }

  getMetaData() {
    if (_.isEmpty(this.serviceId)) {
      return;
    }
    this.rgwDaemonService.get(this.serviceId).then(resp => {
      this.metadata = resp['rgw_metadata'];
    });
  }
}
