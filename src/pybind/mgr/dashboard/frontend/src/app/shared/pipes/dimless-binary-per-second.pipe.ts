import { Pipe, PipeTransform } from '@angular/core';
import { FormatterService } from '../services/formatter.service';

@Pipe({
  name: 'dimlessBinaryPerSecond'
})
export class DimlessBinaryPerSecondPipe implements PipeTransform {
  constructor(private formatter: FormatterService) {}

  transform(value: any): any {
    return this.formatter.format_number(value, 1024, [
      'B/s',
      'kB/s',
      'MB/s',
      'GB/s',
      'TB/s',
      'PB/s',
      'EB/s',
      'ZB/s',
      'YB/s'
    ]);
  }
}
