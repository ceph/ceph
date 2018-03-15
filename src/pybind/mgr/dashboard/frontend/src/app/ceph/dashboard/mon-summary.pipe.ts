import { Pipe, PipeTransform } from '@angular/core';

@Pipe({
  name: 'monSummary'
})
export class MonSummaryPipe implements PipeTransform {
  transform(value: any, args?: any): any {
    if (!value) {
      return '';
    }

    let result = value.monmap.mons.length.toString() + ' (quorum ';
    result += value.quorum.join(', ');
    result += ')';

    return result;
  }
}
