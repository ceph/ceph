import { Pipe, PipeTransform } from '@angular/core';

import _ from 'lodash';

@Pipe({
  name: 'mdsSummary'
})
export class MdsSummaryPipe implements PipeTransform {
  transform(value: any): any {
    if (!value) {
      return {
        success: 0,
        info: 0,
        total: 0
      };
    }

    let activeCount = 0;
    let standbyCount = 0;
    let standbys = 0;
    let active = 0;
    let standbyReplay = 0;
    _.each(value.standbys, () => {
      standbys += 1;
    });

    if (value.standbys && !value.filesystems) {
      standbyCount = standbys;
      activeCount = 0;
    } else if (value.filesystems.length === 0) {
      activeCount = 0;
    } else {
      _.each(value.filesystems, (fs) => {
        _.each(fs.mdsmap.info, (mds) => {
          if (mds.state === 'up:standby-replay') {
            standbyReplay += 1;
          } else {
            active += 1;
          }
        });
      });

      activeCount = active;
      standbyCount = standbys + standbyReplay;
    }
    const totalCount = activeCount + standbyCount;
    const mdsSummary = {
      success: activeCount,
      info: standbyCount,
      total: totalCount
    };

    return mdsSummary;
  }
}
