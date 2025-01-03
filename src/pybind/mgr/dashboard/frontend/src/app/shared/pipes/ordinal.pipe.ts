import { Pipe, PipeTransform } from '@angular/core';

@Pipe({
  name: 'ordinal'
})
export class OrdinalPipe implements PipeTransform {
  transform(value: any): any {
    const num = parseInt(value, 10);
    if (isNaN(num)) {
      return value;
    }
    return (
      value +
      (Math.floor(num / 10) === 1
        ? 'th'
        : num % 10 === 1
        ? 'st'
        : num % 10 === 2
        ? 'nd'
        : num % 10 === 3
        ? 'rd'
        : 'th')
    );
  }
}
