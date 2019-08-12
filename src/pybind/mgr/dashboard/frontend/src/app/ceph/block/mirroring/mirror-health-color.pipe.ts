import { Pipe, PipeTransform } from '@angular/core';

@Pipe({
  name: 'mirrorHealthColor'
})
export class MirrorHealthColorPipe implements PipeTransform {
  transform(value: any): any {
    if (value === 'warning') {
      return 'badge badge-warning';
    } else if (value === 'error') {
      return 'badge badge-danger';
    } else if (value === 'success') {
      return 'badge badge-success';
    }
    return 'badge badge-info';
  }
}
