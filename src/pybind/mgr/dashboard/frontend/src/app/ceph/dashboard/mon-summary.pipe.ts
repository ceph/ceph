import { Pipe, PipeTransform } from '@angular/core';

@Pipe({
  name: 'monSummary'
})
export class MonSummaryPipe implements PipeTransform {
  transform(value: any): any {
    if (!value) {
      return '';
    }

    const result = $localize`${value.monmap.mons.length.toString()} (quorum \
${value.quorum.join(', ')})`;

    return result;
  }
}
