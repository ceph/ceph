import { Injectable } from '@angular/core';

import * as _ from 'lodash';

@Injectable()
export class FormatterService {
  constructor() {}

  truncate(n: number | string, decimals: number): string {
    const value = n.toString();
    const parts = value.split('.');
    if (parts.length === 1) {
      return value; // integer
    } else {
      return Number.parseFloat(value)
        .toPrecision(decimals + parts[0].length)
        .toString()
        .replace(/0+$/, '');
    }
  }

  format_number(n: any, divisor: number, units: string[], decimals: number = 4): string {
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
      result = `${result}${units[unit]}`;
    }
    return result;
  }

  /**
   * Convert the given value into bytes.
   * @param {string} value The value to be converted, e.g. 1024B, 10M, 300KiB or 1ZB.
   * @returns Returns the given value in bytes without any appended unit or null in case
   *   of an error.
   */
  toBytes(value: string): number | null {
    const base = 1024;
    const units = ['b', 'k', 'm', 'g', 't', 'p', 'e', 'z', 'y'];
    const m = RegExp('^(\\d+(.\\d+)?) ?([' + units.join('') + '](b|ib)?)?$', 'i').exec(value);
    if (m === null) {
      return null;
    }
    let bytes = parseFloat(m[1]);
    if (_.isString(m[3])) {
      bytes = bytes * Math.pow(base, units.indexOf(m[3].toLowerCase()[0]));
    }
    return Math.round(bytes);
  }
}
