import { Pipe, PipeTransform } from '@angular/core';
import * as _ from 'lodash';

@Pipe({
  name: 'pgStatus'
})
export class PgStatusPipe implements PipeTransform {
  transform(pgStatus: any, args?: any): any {
    const strings = [];
    _.each(pgStatus, (count, state) => {
      strings.push(count + ' ' + state);
    });

    return strings.join(', ');
  }
}
