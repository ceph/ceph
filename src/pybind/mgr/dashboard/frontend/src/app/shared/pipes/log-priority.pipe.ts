import { Pipe, PipeTransform } from '@angular/core';

@Pipe({
  name: 'logColor'
})
export class LogColorPipe implements PipeTransform {
  transform(value: any, args?: any): any {
    if (value.priority === '[INF]') {
      return ''; // Inherit
    } else if (value.priority === '[WRN]') {
      return {
        color: '#ffa500',
        'font-weight': 'bold'
      };
    } else if (value.priority === '[ERR]') {
      return { color: '#FF2222' };
    } else {
      return '';
    }
  }
}
