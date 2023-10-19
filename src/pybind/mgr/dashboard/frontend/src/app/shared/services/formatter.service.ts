import { Injectable } from '@angular/core';

import _ from 'lodash';

@Injectable({
  providedIn: 'root'
})
export class FormatterService {
  format_number(n: any, divisor: number, units: string[], decimals: number = 1): string {
    if (_.isString(n)) {
      n = Number(n);
    }
    if (!_.isNumber(n)) {
      return '-';
    }
    if (_.isNaN(n)) {
      return 'N/A';
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
   * Converts a value from one set of units to another using a conversion factor
   * @param n The value to be converted
   * @param units The data units of the value
   * @param targetedUnits The wanted data units to convert to
   * @param conversionFactor The factor of convesion
   * @param unitsArray An ordered array containing the data units
   * @param decimals The number of decimals on the returned value
   * @returns Returns a string of the given value formated to the targeted data units.
   */
  formatNumberFromTo(
    n: any,
    units: string = '',
    targetedUnits: string = '',
    conversionFactor: number,
    unitsArray: string[],
    decimals: number = 1
  ): string {
    if (_.isString(n)) {
      n = Number(n);
    }
    if (!_.isNumber(n)) {
      return '-';
    }
    if (!unitsArray) {
      return '-';
    }
    const unitsArrayLowerCase = unitsArray.map((str) => str.toLowerCase());
    if (
      !unitsArrayLowerCase.includes(units.toLowerCase()) ||
      !unitsArrayLowerCase.includes(targetedUnits.toLowerCase())
    ) {
      return `${n} ${units}`;
    }
    const index =
      unitsArrayLowerCase.indexOf(units.toLowerCase()) -
      unitsArrayLowerCase.indexOf(targetedUnits.toLocaleLowerCase());
    const convertedN =
      index > 0
        ? n * Math.pow(conversionFactor, index)
        : n / Math.pow(conversionFactor, Math.abs(index));
    let result = _.round(convertedN, decimals).toString();
    result = `${result} ${targetedUnits}`;
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
  toBytes(value: string, error_value: number = null): number | null {
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

  toOctalPermission(modes: any) {
    const scopes = ['owner', 'group', 'others'];
    let octalMode = '';
    for (const scope of scopes) {
      let scopeValue = 0;
      const mode = modes[scope];

      if (mode) {
        if (mode.includes('read')) scopeValue += 4;
        if (mode.includes('write')) scopeValue += 2;
        if (mode.includes('execute')) scopeValue += 1;
      }

      octalMode += scopeValue.toString();
    }
    return octalMode;
  }
}
