import { Injectable } from '@angular/core';

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

  parseFloat (value, outputSize, defaultInputSize = 'm') {
    let units = ['b', 'k', 'm', 'g', 't', 'p', 'e', 'z', 'y'];
    if (outputSize) {
      units = units.slice(units.indexOf(outputSize));
    }
    if (/^[\d.]+$/.test(value)) {
      value += defaultInputSize;
    }
    value = value && value.toLowerCase().replace(/\s/g, '');
    const rgx = new RegExp('^([\\d.]+)([' + units.join('') + ']?)(i?)(b?)$');
    if (!rgx.test(value)) {
      return null;
    }
    const matched = rgx.exec(value);
    return parseFloat(matched[1]) * Math.pow(1024, units.indexOf(matched[2]));
  }
}
