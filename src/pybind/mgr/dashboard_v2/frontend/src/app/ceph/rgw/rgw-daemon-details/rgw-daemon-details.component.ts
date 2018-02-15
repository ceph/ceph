import { Component, Input, OnInit } from '@angular/core';

import * as _ from 'lodash';

import { RgwDaemonService } from '../services/rgw-daemon.service';

@Component({
  selector: 'cd-rgw-daemon-details',
  templateUrl: './rgw-daemon-details.component.html',
  styleUrls: ['./rgw-daemon-details.component.scss']
})
export class RgwDaemonDetailsComponent implements OnInit {

  metadata: Array<object> = [];
  serviceId = '';

  @Input() selected?: Array<any> = [];

  constructor(private rgwDaemonService: RgwDaemonService) { }

  ngOnInit() {
    // Get the service id of the first selected row.
    if (this.selected.length > 0) {
      this.serviceId = this.selected[0].id;
    }
  }

  getMetaData() {
    if (_.isEmpty(this.serviceId)) {
      return;
    }

    this.rgwDaemonService.get(this.serviceId)
      .then((resp) => {
        const metadata = [];
        const keys = _.keys(resp['rgw_metadata']);
        keys.sort();
        _.map(keys, (key) => {
          metadata.push({
            'key': key,
            'value': resp['rgw_metadata'][key]
          });
        });
        this.metadata = metadata;
      });
  }
}
