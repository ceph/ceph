import { Pipe, PipeTransform } from '@angular/core';

import { I18n } from '@ngx-translate/i18n-polyfill';
import * as _ from 'lodash';

@Pipe({
  name: 'osdSummary'
})
export class OsdSummaryPipe implements PipeTransform {
  constructor(private i18n: I18n) {}

  transform(value: any): any {
    if (!value) {
      return '';
    }

    let inCount = 0;
    let upCount = 0;
    _.each(value.osds, (osd) => {
      if (osd.in) {
        inCount++;
      }
      if (osd.up) {
        upCount++;
      }
    });

    const osdSummary = [
      {
        content: `${value.osds.length} ${this.i18n('total')}`,
        class: ''
      }
    ];
    osdSummary.push({
      content: '',
      class: 'card-text-line-break'
    });
    osdSummary.push({
      content: `${upCount} ${this.i18n('up')}, ${inCount} ${this.i18n('in')}`,
      class: ''
    });

    const downCount = value.osds.length - upCount;
    const outCount = upCount - inCount;
    if (downCount > 0 || outCount > 0) {
      osdSummary.push({
        content: '',
        class: 'card-text-line-break'
      });

      const downText = downCount > 0 ? `${downCount} ${this.i18n('down')}` : '';
      const separator = downCount > 0 && outCount > 0 ? ', ' : '';
      const outText = outCount > 0 ? `${outCount} ${this.i18n('out')}` : '';
      osdSummary.push({
        content: `${downText}${separator}${outText}`,
        class: 'card-text-error'
      });
    }

    return osdSummary;
  }
}
