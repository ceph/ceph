import { Pipe, PipeTransform } from '@angular/core';

import _ from 'lodash';
import moment from 'moment';

moment.updateLocale('en', {
  relativeTime: {
    future: $localize`in %s`,
    past: $localize`%s ago`,
    s: $localize`a few seconds`,
    ss: $localize`%d seconds`,
    m: $localize`a minute`,
    mm: $localize`%d minutes`,
    h: $localize`an hour`,
    hh: $localize`%d hours`,
    d: $localize`a day`,
    dd: $localize`%d days`,
    w: $localize`a week`,
    ww: $localize`%d weeks`,
    M: $localize`a month`,
    MM: $localize`%d months`,
    y: $localize`a year`,
    yy: $localize`%d years`
  }
});

@Pipe({
  name: 'relativeDate',
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
