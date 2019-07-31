import { Pipe, PipeTransform } from '@angular/core';

@Pipe({
  name: 'iscsiBackstore'
})
export class IscsiBackstorePipe implements PipeTransform {
  constructor() {}

  transform(value: any): any {
    switch (value) {
      case 'user:rbd':
        return 'user:rbd (tcmu-runner)';
      default:
        return value;
    }
  }
}
