import { Pipe, PipeTransform } from '@angular/core';

import * as moment from 'moment';

@Pipe({
  name: 'relativeDate'
})
export class RelativeDatePipe implements PipeTransform {
  transform(value: any): any {
    if (!value) {
      return 'unknown';
    }
    return moment(value * 1000).fromNow();
  }
}
