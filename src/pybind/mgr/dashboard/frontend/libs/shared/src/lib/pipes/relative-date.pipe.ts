import { Pipe, PipeTransform } from '@angular/core';

import _ from 'lodash';
import moment from 'moment';

moment.updateLocale('en', {
  relativeTime: {
    future: `in %s`,
    past: `%s ago`,
    s: `a few seconds`,
    ss: `%d seconds`,
    m: `a minute`,
    mm: `%d minutes`,
    h: `an hour`,
    hh: `%d hours`,
    d: `a day`,
    dd: `%d days`,
    w: `a week`,
    ww: `%d weeks`,
    M: `a month`,
    MM: `%d months`,
    y: `a year`,
    yy: `%d years`
  }
});

@Pipe({
  name: 'relativeDate',
  standalone: true,
  pure: false
})
export class RelativeDatePipe implements PipeTransform {
  /**
   * Convert a time into a human readable form, e.g. '2 minutes ago'.
   * @param {Date | string | number} value The date to convert, should be
   *   an ISO8601 string, an Unix timestamp (seconds) or Date object.
   * @param {boolean} upperFirst Set to `true` to start the sentence
   *   upper case. Defaults to `true`.
   * @return {string} The time in human readable form or an empty string
   *   on failure (e.g. invalid input).
   */
  transform(value: Date | string | number, upperFirst = true): string {
    let date: moment.Moment;
    const offset = moment().utcOffset();
    if (_.isNumber(value)) {
      date = moment.parseZone(moment.unix(value)).utc().utcOffset(offset).local();
    } else {
      date = moment.parseZone(value).utc().utcOffset(offset).local();
    }
    if (!date.isValid()) {
      return '';
    }
    let relativeDate: string = date.fromNow();
    if (upperFirst) {
      relativeDate = _.upperFirst(relativeDate);
    }
    return relativeDate;
  }
}
