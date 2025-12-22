import { Pipe, PipeTransform } from '@angular/core';

import _ from 'lodash';

@Pipe({
  name: 'round',
  standalone: false
})
export class RoundPipe implements PipeTransform {
  transform(value: any, precision: number): any {
    return _.round(value, precision);
  }
}
