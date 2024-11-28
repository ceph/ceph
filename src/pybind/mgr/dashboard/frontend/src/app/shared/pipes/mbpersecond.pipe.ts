import { Pipe, PipeTransform } from '@angular/core';

@Pipe({
  name: 'mbpersecond'
})
export class MbpersecondPipe implements PipeTransform {
  transform(value: any): any {
    return `${value} MB/s`;
  }
}
