import { Pipe, PipeTransform } from '@angular/core';

import { CssHelper } from '~/app/shared/classes/css-helper';
import { HealthColor } from '~/app/shared/enum/health-color.enum';

@Pipe({
  name: 'healthColor'
})
export class HealthColorPipe implements PipeTransform {
  constructor(private cssHelper: CssHelper) {}

  transform(value: any): any {
    return Object.keys(HealthColor).includes(value as HealthColor)
      ? { color: this.cssHelper.propertyValue(HealthColor[value]) }
      : null;
  }
}
