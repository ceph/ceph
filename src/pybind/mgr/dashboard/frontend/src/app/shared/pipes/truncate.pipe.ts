import { Pipe, PipeTransform } from '@angular/core';

import * as _ from 'lodash';

@Pipe({
  name: 'truncate'
})
export class TruncatePipe implements PipeTransform {
  transform(value: any, length: number, omission?: string): any {
    if (!_.isString(value)) {
      return value;
    }
    omission = _.defaultTo(omission, '');
    return _.truncate(value, { length, omission });
  }
}
