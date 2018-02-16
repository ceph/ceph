import { Component, Input, OnInit } from '@angular/core';

import * as _ from 'lodash';

import { RgwDaemonService } from '../services/rgw-daemon.service';

@Component({
  selector: 'cd-rgw-daemon-details',
  templateUrl: './rgw-daemon-details.component.html',
  styleUrls: ['./rgw-daemon-details.component.scss']
})
export class RgwDaemonDetailsComponent implements OnInit {

  metadata: any;
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
        this.metadata = resp['rgw_metadata'];
      });
  }
}
