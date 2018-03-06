import { Pipe, PipeTransform } from '@angular/core';

@Pipe({
  name: 'mirrorHealthColor'
})
export class MirrorHealthColorPipe implements PipeTransform {
  transform(value: any, args?: any): any {
    if (value === 'warning') {
      return 'label label-warning';
    } else if (value === 'error') {
      return 'label label-danger';
    } else if (value === 'success') {
      return 'label label-success';
    }
    return 'label label-info';
  }
}
