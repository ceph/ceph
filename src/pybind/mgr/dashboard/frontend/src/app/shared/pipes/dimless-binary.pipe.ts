import { Pipe, PipeTransform } from '@angular/core';

import { FormatterService } from '../services/formatter.service';

@Pipe({
  name: 'dimlessBinary'
})
export class DimlessBinaryPipe implements PipeTransform {
  constructor(private formatter: FormatterService) {}

  transform(value: any): any {
    return this.formatter.format_number(value, 1024, [
      'B',
      'KiB',
      'MiB',
      'GiB',
      'TiB',
      'PiB',
      'EiB',
      'ZiB',
      'YiB'
    ]);
  }
}
