import { Pipe, PipeTransform } from '@angular/core';

import * as _ from 'lodash';

/**
 * Convert the given value to an array.
 */
@Pipe({
  name: 'array'
})
export class ArrayPipe implements PipeTransform {
  /**
   * Convert the given value into an array. If the value is already an
   * array, then nothing happens, except the `force` flag is set.
   * @param value The value to process.
   * @param force Convert the specified value to an array, either it is
   *              already an array.
   */
  transform(value: any, force = false): any[] {
    let result = value;
    if (!_.isArray(value) || (_.isArray(value) && force)) {
      result = [value];
    }
    return result;
  }
}
