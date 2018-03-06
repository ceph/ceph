import { Component, Input, OnChanges } from '@angular/core';

import * as _ from 'lodash';

import { RgwUserService } from '../../../shared/api/rgw-user.service';
import { CdTableSelection } from '../../../shared/models/cd-table-selection';

@Component({
  selector: 'cd-rgw-user-details',
  templateUrl: './rgw-user-details.component.html',
  styleUrls: ['./rgw-user-details.component.scss']
})
export class RgwUserDetailsComponent implements OnChanges {
  user: any;

  @Input() selection: CdTableSelection;

  constructor(private rgwUserService: RgwUserService) {}

  ngOnChanges() {
    if (this.selection.hasSelection) {
      this.user = this.selection.first();

      // Sort subusers and capabilities.
      this.user.subusers = _.sortBy(this.user.subusers, 'id');
      this.user.caps = _.sortBy(this.user.caps, 'type');

      // Load the user/bucket quota of the selected user.
      if (this.user.tenant === '') {
        this.rgwUserService.getQuota(this.user.user_id)
          .subscribe((resp: object) => {
            _.extend(this.user, resp);
          });
      }
    }
  }
}
