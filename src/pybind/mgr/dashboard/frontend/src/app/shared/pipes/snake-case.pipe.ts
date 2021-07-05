import { Pipe, PipeTransform } from '@angular/core';

import _ from 'lodash';

@Pipe({
  name: 'snakeCase'
})
export class SnakeCasePipe implements PipeTransform {
  transform(value: string): string {
    return _.snakeCase(value);
  }
}
