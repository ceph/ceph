import { Pipe, PipeTransform } from '@angular/core';
import * as _ from 'lodash';

@Pipe({
  name: 'pgStatusStyle'
})
export class PgStatusStylePipe implements PipeTransform {
  transform(pgStatus: any, args?: any): any {
    let warning = false;
    let error = false;

    _.each(pgStatus, (value, state) => {
      if (
        state.includes('inconsistent') ||
        state.includes('incomplete') ||
        !state.includes('active')
      ) {
        error = true;
      }

      if (
        state !== 'active+clean' &&
        state !== 'active+clean+scrubbing' &&
        state !== 'active+clean+scrubbing+deep'
      ) {
        warning = true;
      }
    });

    if (error) {
      return { color: '#FF0000' };
    }

    if (warning) {
      return { color: '#FFC200' };
    }

    return { color: '#00BB00' };
  }
}
