import { Pipe, PipeTransform } from '@angular/core';

import _ from 'lodash';

@Pipe({
  name: 'isEmpty'
})
export class IsEmptyPipe implements PipeTransform {
  /**
   * Checks if the given value is an empty object, collection, map, or set.
   * Objects are considered empty if they have no own enumerable string
   * keyed properties. Array-like values such as arguments objects, arrays,
   * buffers or strings are considered empty if they have a length of 0.
   * Similarly, maps and sets are considered empty if they have a size of 0.
   * @param value The value to check.
   * @return Returns `true` if value is empty, else `false`.
   */
  transform(value: any): boolean {
    return _.isEmpty(value);
  }
}
