import { Pipe, PipeTransform } from '@angular/core';

import { I18n } from '@ngx-translate/i18n-polyfill';
import * as _ from 'lodash';

@Pipe({
  name: 'mgrSummary'
})
export class MgrSummaryPipe implements PipeTransform {
  constructor(private i18n: I18n) {}

  transform(value: any, args?: any): any {
    if (!value) {
      return '';
    }

    let result = 'active: ';
    result += _.isUndefined(value.active_name) ? 'n/a' : value.active_name;

    if (value.standbys.length) {
      result += ', ' + value.standbys.length + ' ' + this.i18n('standbys');
    }

    return result;
  }
}
