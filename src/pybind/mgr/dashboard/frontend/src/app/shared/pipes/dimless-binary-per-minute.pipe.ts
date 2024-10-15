import { Pipe, PipeTransform } from '@angular/core';

import { FormatterService } from '../services/formatter.service';

@Pipe({
  name: 'dimlessBinaryPerMinute'
})
export class DimlessBinaryPerMinutePipe implements PipeTransform {
  constructor(private formatter: FormatterService) {}

  transform(value: any, decimals: number = 1): any {
    return this.formatter.format_number(
      value,
      1024,
      ['B/m', 'KiB/m', 'MiB/m', 'GiB/m', 'TiB/m', 'PiB/m', 'EiB/m', 'ZiB/m', 'YiB/m'],
      decimals
    );
  }
}
