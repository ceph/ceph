import { Pipe, PipeTransform } from '@angular/core';

@Pipe({
  name: 'logPriority'
})
export class LogPriorityPipe implements PipeTransform {
  transform(value: any): any {
    if (value === '[INF]') {
      return 'info';
    } else if (value === '[WRN]') {
      return 'warn';
    } else if (value === '[ERR]') {
      return 'err';
    } else {
      return ''; // Inherit
    }
  }
}
