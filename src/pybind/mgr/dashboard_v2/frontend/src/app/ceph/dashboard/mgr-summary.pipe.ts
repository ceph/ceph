import { Pipe, PipeTransform } from '@angular/core';
import * as _ from 'lodash';

@Pipe({
  name: 'mgrSummary'
})
export class MgrSummaryPipe implements PipeTransform {
  transform(value: any, args?: any): any {
    if (!value) {
      return '';
    }

    let result = 'active: ';
    result += _.isUndefined(value.active_name) ? 'n/a' : value.active_name;

    if (value.standbys.length) {
      result += ', ' + value.standbys.length + ' standbys';
    }

    return result;
  }
}
