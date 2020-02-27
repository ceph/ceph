import { Pipe, PipeTransform } from '@angular/core';

import * as _ from 'lodash';

@Pipe({
  name: 'map'
})
export class MapPipe implements PipeTransform {
  transform(value: string | number, map?: object): any {
    if (!_.isPlainObject(map)) {
      return value;
    }
    return _.get(map, value, value);
  }
}
