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

    let contentLine1 = '';
    let contentLine2 = '';
    let standbys = 0;
    let active = 0;
    let standbyReplay = 0;
    _.each(value.standbys, (s, i) => {
      standbys += 1;
    });

    if (value.standbys && !value.filesystems) {
      contentLine1 = `${standbys} ${this.i18n('up')}`;
      contentLine2 = this.i18n('no filesystems');
    } else if (value.filesystems.length === 0) {
      contentLine1 = this.i18n('no filesystems');
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

      contentLine1 = `${active} ${this.i18n('active')}`;
      contentLine2 = `${standbys + standbyReplay} ${this.i18n('standby')}`;
    }

    const mgrSummary = [
      {
        content: contentLine1,
        class: ''
      }
    ];

    if (contentLine2) {
      mgrSummary.push({
        content: '',
        class: 'card-text-line-break'
      });
      mgrSummary.push({
        content: contentLine2,
        class: ''
      });
    }

    return mgrSummary;
  }
}
