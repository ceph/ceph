import { Pipe, PipeTransform } from '@angular/core';

@Pipe({
  name: 'cephReleaseName'
})
export class CephReleaseNamePipe implements PipeTransform {
  transform(value: any): any {
    // Expect "ceph version 13.1.0-419-g251e2515b5
    //         (251e2515b563856349498c6caf34e7a282f62937) nautilus (dev)"
    const result = /ceph version\s+[^ ]+\s+\(.+\)\s+(.+)\s+\((.+)\)/.exec(value);
    if (result) {
      if (result[2] === 'dev') {
        // Assume this is actually master
        return 'master';
      } else {
        // Return the "nautilus" part
        return result[1];
      }
    } else {
      // Unexpected format, pass it through
      return value;
    }
  }
}
