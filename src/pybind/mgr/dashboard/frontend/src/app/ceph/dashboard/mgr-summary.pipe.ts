import { Pipe, PipeTransform } from '@angular/core';

import { I18n } from '@ngx-translate/i18n-polyfill';
import * as _ from 'lodash';

@Pipe({
  name: 'mgrSummary'
})
export class MgrSummaryPipe implements PipeTransform {
  constructor(private i18n: I18n) {}

  transform(value: any): any {
    if (!value) {
      return '';
    }

    let activeCount = this.i18n('n/a');
    const activeTitleText = _.isUndefined(value.active_name)
      ? ''
      : `${this.i18n('active daemon')}: ${value.active_name}`;
    // There is always one standbyreplay to replace active daemon, if active one is down
    if (activeTitleText.length > 0) {
      activeCount = '1';
    }
    const standbyHoverText = value.standbys.map((s: any): string => s.name).join(', ');
    const standbyTitleText = !standbyHoverText
      ? ''
      : `${this.i18n('standby daemons')}: ${standbyHoverText}`;
    const standbyCount = value.standbys.length;
    const mgrSummary = [
      {
        content: `${activeCount} ${this.i18n('active')}`,
        class: 'popover-info',
        titleText: activeTitleText
      }
    ];

    mgrSummary.push({
      content: '',
      class: 'card-text-line-break',
      titleText: ''
    });
    mgrSummary.push({
      content: `${standbyCount} ${this.i18n('standby')}`,
      class: 'popover-info',
      titleText: standbyTitleText
    });

    return mgrSummary;
  }
}
