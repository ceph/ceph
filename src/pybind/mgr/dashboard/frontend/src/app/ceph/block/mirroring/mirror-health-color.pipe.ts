import { Pipe, PipeTransform } from '@angular/core';

@Pipe({
  name: 'mirrorHealthColor'
})
export class MirrorHealthColorPipe implements PipeTransform {
  transform(value: any): any {
    if (value === 'warning') {
      return 'tags-warning';
    } else if (value === 'error') {
      return 'tags-danger';
    } else if (value === 'success') {
      return 'tags-success';
    }
    return 'tags-info';
  }
}
