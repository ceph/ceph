import { Pipe, PipeTransform } from '@angular/core';
import { VERSION_PREFIX } from '~/app/shared/constants/app.constants';

@Pipe({
  name: 'cephVersion',
  standalone: false
})
export class CephVersionPipe implements PipeTransform {
  transform(value: string = ''): string {
    // Expect "ceph version 13.1.0-419-g251e2515b5
    //         (251e2515b563856349498c6caf34e7a282f62937) nautilus (dev)"
    if (value) {
      const version = value.replace(`${VERSION_PREFIX} `, '').split(' ');
      return version[0] + ' ' + version.slice(2, version.length).join(' ');
    }

    return value;
  }
}
