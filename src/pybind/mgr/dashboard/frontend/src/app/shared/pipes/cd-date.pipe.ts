import { Pipe, PipeTransform } from '@angular/core';
import _ from 'lodash';
import moment from 'moment';

@Pipe({
  name: 'cdDate',
  standalone: false
})
export class CdDatePipe implements PipeTransform {
  private static readonly DEFAULT_FORMAT = 'D/M/YY hh:mm A';

  constructor() {}

  transform(value: any, format: string = CdDatePipe.DEFAULT_FORMAT): any {
    if (value === null || value === '') {
      return '';
    }
    let date: string;
    const offset = moment().utcOffset();
    if (_.isNumber(value)) {
      date = moment.parseZone(moment.unix(value)).utc().utcOffset(offset).local().format(format);
    } else {
      value = value?.replace?.('Z', '');
      date = moment.parseZone(value).utc().utcOffset(offset).local().format(format);
    }
    return date;
  }
}
