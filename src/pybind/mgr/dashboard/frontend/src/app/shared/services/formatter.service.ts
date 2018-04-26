import { Injectable } from '@angular/core';

import * as _ from 'lodash';

@Injectable()
export class FormatterService {
  constructor() {}

  truncate(n, maxWidth) {
    const stringized = n.toString();
    const parts = stringized.split('.');
    if (parts.length === 1) {
      // Just an int
      return stringized;
    } else {
      const fractionalDigits = maxWidth - parts[0].length - 1;
      if (fractionalDigits <= 0) {
        // No width available for the fractional part, drop
        // it and the decimal point
        return parts[0];
      } else {
        return stringized.substring(0, maxWidth);
      }
    }
  }

  format_number(n, divisor, units) {
    const width = 4;
    let unit = 0;

    if (n == null) {
      // People shouldn't really be passing null, but let's
      // do something sensible instead of barfing.
      return '-';
    }

    while (Math.floor(n / divisor ** unit).toString().length > width - 1) {
      unit = unit + 1;
    }

    let truncatedFloat;
    if (unit > 0) {
      truncatedFloat = this.truncate(
        (n / Math.pow(divisor, unit)).toString(),
        width
      );
    } else {
      truncatedFloat = this.truncate(n, width);
    }

    return truncatedFloat === '' ? '-' : (truncatedFloat + units[unit]);
  }

  /**
   * Convert the given value into bytes.
   * @param {string} value The value to be converted, e.g. 1024B, 10M, 300KiB or 1ZB.
   * @returns Returns the given value in bytes without any appended unit or null in case
   *   of an error.
   */
  toBytes(value: string): number | null {
    const base = 1024;
    const units = {
      'b': 1,
      'k': Math.pow(base, 1),
      'kb': Math.pow(base, 1),
      'kib': Math.pow(base, 1),
      'm': Math.pow(base, 2),
      'mb': Math.pow(base, 2),
      'mib': Math.pow(base, 2),
      'g': Math.pow(base, 3),
      'gb': Math.pow(base, 3),
      'gib': Math.pow(base, 3),
      't': Math.pow(base, 4),
      'tb': Math.pow(base, 4),
      'tib': Math.pow(base, 4),
      'p': Math.pow(base, 5),
      'pb': Math.pow(base, 5),
      'pib': Math.pow(base, 5),
      'e': Math.pow(base, 6),
      'eb': Math.pow(base, 6),
      'eib': Math.pow(base, 6),
      'z': Math.pow(base, 7),
      'zb': Math.pow(base, 7),
      'zib': Math.pow(base, 7),
      'y': Math.pow(base, 8),
      'yb': Math.pow(base, 8),
      'yib': Math.pow(base, 8)
    };
    const m = RegExp('^(\\d+(\.\\d+)?)\\s*(B|K(B|iB)?|M(B|iB)?|G(B|iB)?|T(B|iB)?|P(B|iB)?|' +
      'E(B|iB)?|Z(B|iB)?|Y(B|iB)?)?$', 'i').exec(value);
    if (m === null) {
      return null;
    }
    let bytes = parseFloat(m[1]);
    if (_.isString(m[3])) {
      bytes = bytes * units[m[3].toLowerCase()];
    }
    return Math.floor(bytes);
  }
}
