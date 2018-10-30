import { Pipe, PipeTransform } from '@angular/core';

import { I18n } from '@ngx-translate/i18n-polyfill';
import * as _ from 'lodash';

@Pipe({
  name: 'mdsSummary'
})
export class MdsSummaryPipe implements PipeTransform {
  constructor(private i18n: I18n) {}

  transform(value: any, args?: any): any {
    if (!value) {
      return '';
    }

    let standbys = 0;
    let active = 0;
    let standbyReplay = 0;
    _.each(value.standbys, (s, i) => {
      standbys += 1;
    });

    if (value.standbys && !value.filesystems) {
      return standbys + ', ' + this.i18n('no filesystems');
    } else if (value.filesystems.length === 0) {
      return this.i18n('no filesystems');
    } else {
      _.each(value.filesystems, (fs, i) => {
        _.each(fs.mdsmap.info, (mds, j) => {
          if (mds.state === 'up:standby-replay') {
            standbyReplay += 1;
          } else {
            active += 1;
          }
        });
      });

      return `${active} ${this.i18n('active')}, ${standbys + standbyReplay} ${this.i18n(
        'standby'
      )}`;
    }
  }
}
