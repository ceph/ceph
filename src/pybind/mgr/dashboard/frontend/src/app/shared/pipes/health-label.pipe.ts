import { Pipe, PipeTransform } from '@angular/core';

import { HealthLabel } from '~/app/shared/enum/health-label.enum';

@Pipe({
  name: 'healthLabel'
})
export class HealthLabelPipe implements PipeTransform {
  transform(value: any): any {
    return Object.keys(HealthLabel).includes(value as HealthLabel) ? HealthLabel[value] : null;
  }
}
