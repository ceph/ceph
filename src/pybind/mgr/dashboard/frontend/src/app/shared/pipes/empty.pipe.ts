import { Pipe, PipeTransform } from '@angular/core';

import * as _ from 'lodash';

@Pipe({
  name: 'empty'
})
export class EmptyPipe implements PipeTransform {
  transform(value: any): any {
    return _.isUndefined(value) || _.isNull(value) ? '-' : value;
  }
}
