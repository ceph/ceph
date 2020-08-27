import { Pipe, PipeTransform } from '@angular/core';

@Pipe({
  name: 'function'
})
export class FunctionPipe implements PipeTransform {
  transform(value: any, func: (value: any) => any, ...args: any[]): any {
    return func.call(null, ...[value, ...args]);
  }
}
