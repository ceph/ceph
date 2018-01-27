import { Pipe, PipeTransform } from '@angular/core';

@Pipe({
  name: 'serviceList'
})
export class ServiceListPipe implements PipeTransform {
  transform(value: any, args?: any): any {
    const strings = [];
    value.forEach((server) => {
      strings.push(server.type + '.' + server.id);
    });
    return strings.join(', ');
  }
}
