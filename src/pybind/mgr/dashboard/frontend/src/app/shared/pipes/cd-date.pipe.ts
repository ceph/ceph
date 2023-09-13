import { DatePipe } from '@angular/common';
import { Pipe, PipeTransform } from '@angular/core';

@Pipe({
  name: 'cdDate'
})
export class CdDatePipe implements PipeTransform {
  constructor(private datePipe: DatePipe) {}

  transform(value: any): any {
    if (value === null || value === '') {
      return '';
    }
    return (
      this.datePipe.transform(value, 'shortDate') +
      ' ' +
      this.datePipe.transform(value, 'mediumTime')
    );
  }
}
