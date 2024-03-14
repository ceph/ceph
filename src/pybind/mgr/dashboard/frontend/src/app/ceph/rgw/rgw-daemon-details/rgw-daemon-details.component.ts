import { Component, Input, OnChanges } from '@angular/core';

import _ from 'lodash';

import { RgwDaemon } from '~/app/ceph/rgw/models/rgw-daemon';
import { RgwDaemonService } from '~/app/shared/api/rgw-daemon.service';
import { Permission } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';

@Component({
  selector: 'cd-rgw-daemon-details',
  templateUrl: './rgw-daemon-details.component.html',
  styleUrls: ['./rgw-daemon-details.component.scss']
})
export class RgwDaemonDetailsComponent implements OnChanges {
  metadata: any;
  serviceId = '';
  serviceMapId = '';
  grafanaPermission: Permission;

  @Input()
  selection: RgwDaemon;

  constructor(
    private rgwDaemonService: RgwDaemonService,
    private authStorageService: AuthStorageService
  ) {
    this.grafanaPermission = this.authStorageService.getPermissions().grafana;
  }

  ngOnChanges() {
    if (this.selection) {
      this.serviceId = this.selection.id;
      this.serviceMapId = this.selection.service_map_id;
    }
  }

  getMetaData() {
    if (_.isEmpty(this.serviceId)) {
      return;
    }
    this.rgwDaemonService.get(this.serviceId).subscribe((resp: any) => {
      this.metadata = resp['rgw_metadata'];
    });
  }
}
