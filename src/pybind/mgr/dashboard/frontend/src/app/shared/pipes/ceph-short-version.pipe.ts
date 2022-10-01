import { Pipe, PipeTransform } from '@angular/core';

@Pipe({
  name: 'cephShortVersion'
})
export class CephShortVersionPipe implements PipeTransform {
  transform(value: any): any {
    // Expect "ceph version 1.2.3-g9asdasd (as98d7a0s8d7)"
    const result = /ceph version\s+([^ ]+)\s+\(.+\)/.exec(value);
    if (result) {
      // Return the "1.2.3-g9asdasd" part
      return result[1];
    } else {
      // Unexpected format, pass it through
      return value;
    }
  }
}
