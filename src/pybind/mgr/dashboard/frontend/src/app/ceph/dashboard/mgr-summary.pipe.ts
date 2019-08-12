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
    const titleText = _.isUndefined(value.active_name)
      ? ''
      : `${this.i18n('active daemon')}: ${value.active_name}`;
    if (titleText.length > 0) {
      activeCount = '1';
    }
    const standbyCount = value.standbys.length;
    const mgrSummary = [
      {
        content: `${activeCount} ${this.i18n('active')}`,
        class: 'mgr-active-name',
        titleText: titleText
      }
    ];

    mgrSummary.push({
      content: '',
      class: 'card-text-line-break',
      titleText: ''
    });
    mgrSummary.push({
      content: `${standbyCount} ${this.i18n('standby')}`,
      class: '',
      titleText: ''
    });

    return mgrSummary;
  }
}
