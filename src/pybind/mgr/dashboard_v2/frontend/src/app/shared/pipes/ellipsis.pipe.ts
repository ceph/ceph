import { Pipe, PipeTransform } from '@angular/core';

@Pipe({
  name: 'ellipsis'
})
export class EllipsisPipe implements PipeTransform {

  transform(value: any, args?: any): any {
    let result = '';
    for (let i = 0; i < value; i++) {
      result += '...';
    }
    return result;
  }

}
