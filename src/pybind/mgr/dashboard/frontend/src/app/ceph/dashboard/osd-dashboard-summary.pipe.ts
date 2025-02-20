import { Pipe, PipeTransform } from '@angular/core';

import _ from 'lodash';

@Pipe({
  name: 'osdDashboardSummary'
})
export class osdDashboardSummaryPipe implements PipeTransform {
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

    const osdSummary = [
      {
        content: `${value.osds.length} ${$localize`total`}`,
        class: ''
      }
    ];
    osdSummary.push({
      content: '',
      class: 'card-text-line-break'
    });
    osdSummary.push({
      content: `${upCount} ${$localize`up`}, ${inCount} ${$localize`in`}`,
      class: ''
    });

    const downCount = value.osds.length - upCount;
    const outCount = value.osds.length - inCount;
    if (downCount > 0 || outCount > 0) {
      osdSummary.push({
        content: '',
        class: 'card-text-line-break'
      });

      const downText = downCount > 0 ? `${downCount} ${$localize`down`}` : '';
      const separator = downCount > 0 && outCount > 0 ? ', ' : '';
      const outText = outCount > 0 ? `${outCount} ${$localize`out`}` : '';
      osdSummary.push({
        content: `${downText}${separator}${outText}`,
        class: 'card-text-error'
      });
    }

    if (nearFullCount > 0) {
      osdSummary.push(
        {
          content: '',
          class: 'card-text-line-break'
        },
        {
          content: `${nearFullCount} ${$localize`near full`}`,
          class: 'card-text-error'
        },
        {
          content: '',
          class: 'card-text-line-break'
        }
      );
    }

    if (fullCount > 0) {
      osdSummary.push({
        content: `${fullCount} ${$localize`full`}`,
        class: 'card-text-error'
      });
    }

    return osdSummary;
  }
}
