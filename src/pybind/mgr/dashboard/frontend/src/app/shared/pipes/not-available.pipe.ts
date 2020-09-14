import { Pipe, PipeTransform } from '@angular/core';

@Pipe({
  name: 'notAvailable'
})
export class NotAvailablePipe implements PipeTransform {
  transform(value: any): any {
    if (value === '') {
      return $localize`n/a`;
    }
    return value;
  }
}
