import { Pipe, PipeTransform } from '@angular/core';

import _ from 'lodash';

@Pipe({
  name: 'notAvailable'
})
export class NotAvailablePipe implements PipeTransform {
  transform(value: any, text?: string): any {
    if (value === '') {
      return _.defaultTo(text, $localize`n/a`);
    }
    return value;
  }
}
