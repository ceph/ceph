import { Injectable } from '@angular/core';

import * as _ from 'lodash';

import { ServicesModule } from './services.module';

@Injectable({
  providedIn: ServicesModule
})
export class FormatterService {
  constructor() {}

  format_number(n: any, divisor: number, units: string[], decimals: number = 1): string {
    if (_.isString(n)) {
      n = Number(n);
    }
    if (!_.isNumber(n)) {
      return '-';
    }
    let unit = n < 1 ? 0 : Math.floor(Math.log(n) / Math.log(divisor));
    unit = unit >= units.length ? units.length - 1 : unit;
    let result = _.round(n / Math.pow(divisor, unit), decimals).toString();
    if (result === '') {
      return '-';
    }
    if (units[unit] !== '') {
      result = `${result} ${units[unit]}`;
    }
    return result;
  }

  /**
   * Convert the given value into bytes.
   * @param {string} value The value to be converted, e.g. 1024B, 10M, 300KiB or 1ZB.
   * @param error_value The value returned in case the regular expression did not match. Defaults to
   *                    null.
   * @returns Returns the given value in bytes without any unit appended or the defined error value
   *          in case xof an error.
   */
  toBytes(value: string, error_value = null): number | null {
    const base = 1024;
    const units = ['b', 'k', 'm', 'g', 't', 'p', 'e', 'z', 'y'];
    const m = RegExp('^(\\d+(.\\d+)?) ?([' + units.join('') + ']?(b|ib|B/s)?)?$', 'i').exec(value);
    if (m === null) {
      return error_value;
    }
    let bytes = parseFloat(m[1]);
    if (_.isString(m[3])) {
      bytes = bytes * Math.pow(base, units.indexOf(m[3].toLowerCase()[0]));
    }
    return Math.round(bytes);
  }

  /**
   * Converts `x ms` to `x` (currently) or `0` if the conversion fails
   */
  toMilliseconds(value: string): number {
    const pattern = /^\s*(\d+)\s*(ms)?\s*$/i;
    const testResult = pattern.exec(value);

    if (testResult !== null) {
      return +testResult[1];
    }

    return 0;
  }

  /**
   * Converts `x IOPS` to `x` (currently) or `0` if the conversion fails
   */
  toIops(value: string): number {
    const pattern = /^\s*(\d+)\s*(IOPS)?\s*$/i;
    const testResult = pattern.exec(value);

    if (testResult !== null) {
      return +testResult[1];
    }

    return 0;
  }
}
