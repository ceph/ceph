import { Pipe, PipeTransform } from '@angular/core';

@Pipe({
  name: 'pluralize'
})
export class PluralizePipe implements PipeTransform {
  transform(value: string, count?: number): string {
    if (count <= 1) {
      return value;
    }
    if (value.endsWith('y')) {
      return value.slice(0, -1) + 'ies';
    } else {
      return value + 's';
    }
  }
}
