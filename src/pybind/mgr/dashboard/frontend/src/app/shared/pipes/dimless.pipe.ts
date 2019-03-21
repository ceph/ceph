import { Pipe, PipeTransform } from '@angular/core';
import { FormatterService } from '../services/formatter.service';

import * as _ from 'lodash';

@Pipe({
  name: 'dimless'
})
export class DimlessPipe implements PipeTransform {
  constructor(private formatter: FormatterService) {}

  transform(value: any, decimals?: number): any {
    if (_.isUndefined(decimals)) {
      decimals = 4;
    }

    return this.formatter.format_number(value, 1000, [
      '',
      'k',
      'M',
      'G',
      'T',
      'P',
      'E',
      'Z',
      'Y'
    ], decimals);
  }
}
