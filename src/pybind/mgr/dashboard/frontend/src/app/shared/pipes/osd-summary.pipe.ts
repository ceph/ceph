import { Pipe, PipeTransform } from '@angular/core';

import _ from 'lodash';

@Pipe({
  name: 'osdSummary'
})
export class OsdSummaryPipe implements PipeTransform {
  transform(value: any): any {
    if (!value) {
      return '';
    }

    let inCount = 0;
    let upCount = 0;
    let nearFullCount = 0;
    let fullCount = 0;
    _.each(value.osds, (osd) => {
      if (osd.in) {
        inCount++;
      }
      if (osd.up) {
        upCount++;
      }
      if (osd.state.includes('nearfull')) {
        nearFullCount++;
      }
      if (osd.state.includes('full')) {
        fullCount++;
      }
    });

    const downCount = value.osds.length - upCount;
    const outCount = value.osds.length - inCount;
    const osdSummary = {
      total: value.osds.length,
      down: downCount,
      out: outCount,
      up: upCount,
      in: inCount,
      nearfull: nearFullCount,
      full: fullCount
    };
    return osdSummary;
  }
}
