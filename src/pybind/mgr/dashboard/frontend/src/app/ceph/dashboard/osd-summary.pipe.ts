import { Pipe, PipeTransform } from '@angular/core';
import * as _ from 'lodash';

@Pipe({
  name: 'osdSummary'
})
export class OsdSummaryPipe implements PipeTransform {
  transform(value: any, args?: any): any {
    if (!value) {
      return '';
    }

    let inCount = 0;
    let upCount = 0;
    _.each(value.osds, (osd, i) => {
      if (osd.in) {
        inCount++;
      }
      if (osd.up) {
        upCount++;
      }
    });

    return value.osds.length + ' (' + upCount + ' up, ' + inCount + ' in)';
  }
}
