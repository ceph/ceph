import { Pipe, PipeTransform } from '@angular/core';

import * as _ from 'lodash';

@Pipe({
  name: 'upperFirst'
})
export class UpperFirstPipe implements PipeTransform {
  transform(value: string): string {
    return _.upperFirst(value);
  }
}
