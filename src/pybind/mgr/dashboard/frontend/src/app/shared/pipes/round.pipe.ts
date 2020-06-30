import { Pipe, PipeTransform } from '@angular/core';

import * as _ from 'lodash';

@Pipe({
  name: 'round'
})
export class RoundPipe implements PipeTransform {
  transform(value: any, precision: number): any {
    return _.round(value, precision);
  }
}
