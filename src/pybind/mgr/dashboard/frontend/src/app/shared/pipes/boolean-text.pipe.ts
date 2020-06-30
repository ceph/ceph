import { Pipe, PipeTransform } from '@angular/core';

import { I18n } from '@ngx-translate/i18n-polyfill';

@Pipe({
  name: 'booleanText'
})
export class BooleanTextPipe implements PipeTransform {
  constructor(private i18n: I18n) {}

  transform(
    value: any,
    truthyText: string = this.i18n('Yes'),
    falsyText: string = this.i18n('No')
  ): string {
    return Boolean(value) ? truthyText : falsyText;
  }
}
