import { Pipe, PipeTransform } from '@angular/core';

@Pipe({
  name: 'healthColor'
})
export class HealthColorPipe implements PipeTransform {
  transform(value: any): any {
    if (value === 'HEALTH_OK') {
      return { color: '#00bb00' };
    } else if (value === 'HEALTH_WARN') {
      return { color: '#ffa500' };
    } else if (value === 'HEALTH_ERR') {
      return { color: '#ff0000' };
    } else {
      return null;
    }
  }
}
