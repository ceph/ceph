import { Component, Input, OnChanges } from '@angular/core';
import _ from 'lodash';
import { Observable } from 'rxjs';

import { RgwBucketService } from '~/app/shared/api/rgw-bucket.service';
import { RgwDaemonService } from '~/app/shared/api/rgw-daemon.service';
import { RgwUserService } from '~/app/shared/api/rgw-user.service';
import { RgwDaemon } from '../models/rgw-daemon';

@Component({
  selector: 'cd-rgw-bucket-details',
  templateUrl: './rgw-bucket-details.component.html',
  styleUrls: ['./rgw-bucket-details.component.scss']
})
export class RgwBucketDetailsComponent implements OnChanges {
  @Input()
  selection: any;
  user: any;

  // Keys tab
  keys: any = [];
  selectedDaemon: Observable<RgwDaemon>;
  endpoint: string;
  zonegroupName: string;

  constructor(
    private rgwBucketService: RgwBucketService,
    private rgwUserService: RgwUserService,
    private rgwDaemonService: RgwDaemonService
  ) {}

  ngOnChanges() {
    if (this.selection) {
      this.selectedDaemon = this.rgwDaemonService.selectedDaemon$;
      this.rgwBucketService.get(this.selection.bid).subscribe((bucket: any) => {
        bucket['lock_retention_period_days'] = this.rgwBucketService.getLockDays(bucket);
        this.selection = bucket;

        this.rgwUserService.get(bucket.owner).subscribe((resp: object) => {
          this.user = resp;
          this.keys = [];
          if (this.user?.keys) {
            this.user.keys.forEach((key: any) => {
              this.keys.push({
                id: this.keys.length + 1, // Create an unique identifier
                username: key.user,
                ref: key
              });
            });
          }
          this.keys = _.sortBy(this.keys, 'user');
          getHostNamePort();
        });
      });
    }

    const getHostNamePort = () =>
      this.selectedDaemon.subscribe((values) => {
        this.zonegroupName = values?.zonegroup_name;
        this.endpoint = 'http://' + values?.server_hostname + ':' + values?.port;
      });
  }
}
