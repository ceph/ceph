import { Pipe, PipeTransform } from '@angular/core';
import * as _ from 'lodash';

@Pipe({
  name: 'osdSummary'
})
export class OsdSummaryPipe implements PipeTransform {
  static readonly COLOR_ERROR = '#ff0000';

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

    const osdSummary = [
      {
        content: `${value.osds.length} (${upCount} up, ${inCount} in`,
        style: { 'margin-right': '-5px', color: '' }
      }
    ];

    const downCount = value.osds.length - upCount;
    if (downCount > 0) {
      osdSummary.push({
        content: ', ',
        style: { 'margin-right': '0', color: '' }
      });
      osdSummary.push({
        content: `${downCount} down`,
        style: { 'margin-right': '-5px', color: OsdSummaryPipe.COLOR_ERROR }
      });
    }

    osdSummary.push({
      content: ')',
      style: { 'margin-right': '0', color: '' }
    });

    return osdSummary;
  }
}
