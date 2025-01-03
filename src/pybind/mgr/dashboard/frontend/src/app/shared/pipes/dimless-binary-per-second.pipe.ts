import { Pipe, PipeTransform } from '@angular/core';

import { FormatterService } from '../services/formatter.service';

@Pipe({
  name: 'dimlessBinaryPerSecond'
})
export class DimlessBinaryPerSecondPipe implements PipeTransform {
  constructor(private formatter: FormatterService) {}

  transform(value: any, decimals: number = 1): any {
    return this.formatter.format_number(
      value,
      1024,
      ['B/s', 'KiB/s', 'MiB/s', 'GiB/s', 'TiB/s', 'PiB/s', 'EiB/s', 'ZiB/s', 'YiB/s'],
      decimals
    );
  }
}
