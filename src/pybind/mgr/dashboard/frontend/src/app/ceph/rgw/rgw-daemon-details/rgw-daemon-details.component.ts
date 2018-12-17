import { Component, Input, OnChanges } from '@angular/core';

import * as _ from 'lodash';

import { RgwDaemonService } from '../../../shared/api/rgw-daemon.service';
import { CdTableSelection } from '../../../shared/models/cd-table-selection';
import { Permission } from '../../../shared/models/permissions';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';

@Component({
  selector: 'cd-rgw-daemon-details',
  templateUrl: './rgw-daemon-details.component.html',
  styleUrls: ['./rgw-daemon-details.component.scss']
})
export class RgwDaemonDetailsComponent implements OnChanges {
  metadata: any;
  serviceId = '';
  grafanaPermission: Permission;

  @Input()
  selection: CdTableSelection;

  constructor(
    private rgwDaemonService: RgwDaemonService,
    private authStorageService: AuthStorageService
  ) {
    this.grafanaPermission = this.authStorageService.getPermissions().grafana;
  }

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
    this.rgwDaemonService.get(this.serviceId).subscribe((resp) => {
      this.metadata = resp['rgw_metadata'];
    });
  }
}
