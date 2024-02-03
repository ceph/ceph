import { Pipe, PipeTransform } from '@angular/core';

import { FormatterService } from '../services/formatter.service';

@Pipe({
  name: 'dimless'
})
export class DimlessPipe implements PipeTransform {
  constructor(private formatter: FormatterService) {}

  transform(value: any, decimals: number = 1): any {
    return this.formatter.format_number(
      value,
      1000,
      ['', 'k', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y'],
      decimals
    );
  }
}
