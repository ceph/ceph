import { Component, Input, OnChanges } from '@angular/core';

import * as _ from 'lodash';

import { OsdService } from '../../../../shared/api/osd.service';
import { Permission } from '../../../../shared/models/permissions';
import { AuthStorageService } from '../../../../shared/services/auth-storage.service';

@Component({
  selector: 'cd-osd-details',
  templateUrl: './osd-details.component.html',
  styleUrls: ['./osd-details.component.scss']
})
export class OsdDetailsComponent implements OnChanges {
  @Input()
  selection: any;

  osd: {
    id?: number;
    loaded?: boolean;
    details?: any;
    histogram_failed?: string;
    tree?: any;
  };
  grafanaPermission: Permission;

  constructor(private osdService: OsdService, private authStorageService: AuthStorageService) {
    this.grafanaPermission = this.authStorageService.getPermissions().grafana;
  }

  ngOnChanges() {
    this.osd = {
      loaded: false
    };
    if (this.selection) {
      this.osd = this.selection;
      this.refresh();
    }
  }

  refresh() {
    this.osdService.getDetails(this.osd.id).subscribe((data) => {
      this.osd.details = data;
      this.osd.histogram_failed = '';
      if (!_.isObject(data.histogram)) {
        this.osd.histogram_failed = data.histogram;
        this.osd.details.histogram = undefined;
      }
      this.osd.loaded = true;
    });
  }
}
