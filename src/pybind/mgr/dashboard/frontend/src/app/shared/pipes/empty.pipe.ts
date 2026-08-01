import { Pipe, PipeTransform } from '@angular/core';

import _ from 'lodash';

@Pipe({
  name: 'empty',
  standalone: false
})
export class EmptyPipe implements PipeTransform {
  transform(value: any, emptyText?: string): any {
    if (_.isUndefined(value) || _.isNull(value) || value === '') {
      return emptyText ?? '-';
    } else if (_.isNaN(value)) {
      return 'N/A';
    }
    return value;
  }
}
