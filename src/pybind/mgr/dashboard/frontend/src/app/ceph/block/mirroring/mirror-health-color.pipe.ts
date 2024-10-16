import { Pipe, PipeTransform } from '@angular/core';

@Pipe({
  name: 'mirrorHealthColor'
})
export class MirrorHealthColorPipe implements PipeTransform {
  transform(value: any): any {
    if (value === 'warning') {
      return 'tag-warning';
    } else if (value === 'error') {
      return 'tag-danger';
    } else if (value === 'success') {
      return 'tag-success';
    }
    return 'tag-info';
  }
}
