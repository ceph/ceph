import { Pipe, PipeTransform } from '@angular/core';

@Pipe({
  name: 'filter'
})
export class FilterPipe implements PipeTransform {
  transform(value: any, args?: any): any {
    return value.filter((row: any) => {
      let result = true;

      args.forEach((filter: any): boolean | void => {
        if (!filter.value) {
          return undefined;
        }

        result = result && filter.applyFilter(row, filter.value);
        if (!result) {
          return result;
        }
      });

      return result;
    });
  }
}
