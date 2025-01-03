import { Pipe, PipeTransform } from '@angular/core';

@Pipe({
  name: 'pluralize'
})
export class PluralizePipe implements PipeTransform {
  transform(value: string): string {
    if (value.endsWith('y')) {
      return value.slice(0, -1) + 'ies';
    } else {
      return value + 's';
    }
  }
}
