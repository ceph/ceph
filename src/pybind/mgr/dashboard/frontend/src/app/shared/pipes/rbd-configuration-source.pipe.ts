import { Pipe, PipeTransform } from '@angular/core';

@Pipe({
  name: 'rbdConfigurationSource',
  standalone: false
})
export class RbdConfigurationSourcePipe implements PipeTransform {
  transform(value: any): any {
    const sourceMap = {
      0: 'global',
      1: 'pool',
      2: 'image'
    };
    return sourceMap[value];
  }
}
