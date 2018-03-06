import { Pipe, PipeTransform } from '@angular/core';

@Pipe({
  name: 'filter'
})
export class FilterPipe implements PipeTransform {
  transform(value: any, args?: any): any {
    return value.filter(row => {
      let result = true;

      args.forEach(filter => {
        if (!filter.value) {
          return;
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
