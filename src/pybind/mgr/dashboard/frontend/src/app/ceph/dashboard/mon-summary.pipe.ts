import { Pipe, PipeTransform } from '@angular/core';

import { I18n } from '@ngx-translate/i18n-polyfill';

@Pipe({
  name: 'monSummary'
})
export class MonSummaryPipe implements PipeTransform {
  constructor(private i18n: I18n) {}

  transform(value: any): any {
    if (!value) {
      return '';
    }

    const result = `${value.monmap.mons.length.toString()} (${this.i18n(
      'quorum'
    )} ${value.quorum.join(', ')})`;

    return result;
  }
}
