import { Pipe, PipeTransform } from '@angular/core';

import { HealthIcon } from '../enum/health-icon.enum';

@Pipe({
  name: 'healthIcon'
})
export class HealthIconPipe implements PipeTransform {
  transform(value: string): string {
    return Object.keys(HealthIcon).includes(value as HealthIcon) ? HealthIcon[value] : '';
  }
}
