import { Pipe, PipeTransform } from '@angular/core';

import _ from 'lodash';

@Pipe({
  name: 'empty'
})
export class EmptyPipe implements PipeTransform {
  transform(value: any): any {
    if (_.isUndefined(value) || _.isNull(value)) {
      return '-';
    } else if (_.isNaN(value)) {
      return 'N/A';
    }
    return value;
  }
}
