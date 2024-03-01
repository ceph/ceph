import { Pipe, PipeTransform } from '@angular/core';
import _ from 'lodash';
import moment from 'moment';

@Pipe({
  name: 'cdDate'
})
export class CdDatePipe implements PipeTransform {
  constructor() {}

  transform(value: any): any {
    if (value === null || value === '') {
      return '';
    }
    let date: string;
    const offset = moment().utcOffset();
    if (_.isNumber(value)) {
      date = moment
        .parseZone(moment.unix(value))
        .utc()
        .utcOffset(offset)
        .local()
        .format('D/M/YY hh:mm A');
    } else {
      value = value?.replace('Z', '');
      date = moment.parseZone(value).utc().utcOffset(offset).local().format('D/M/YY hh:mm A');
    }
    return date;
  }
}
