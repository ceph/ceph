import { Pipe, PipeTransform } from '@angular/core';
import { HealthIcon } from '../enum/health-icon.enum';

@Pipe({
  name: 'healthIcon'
})
export class HealthIconPipe implements PipeTransform {

  constructor() {}

  transform(value: any): any {
    return Object.keys(HealthIcon).includes(value as HealthIcon)
      ? HealthIcon[value]
      : null;
  }
}
