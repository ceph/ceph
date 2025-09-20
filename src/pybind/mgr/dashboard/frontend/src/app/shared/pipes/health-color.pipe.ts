import { Pipe, PipeTransform } from '@angular/core';

import { CssHelper } from '~/app/shared/classes/css-helper';
import { HealthColor } from '~/app/shared/enum/health-color.enum';

@Pipe({
  name: 'healthColor'
})
export class HealthColorPipe implements PipeTransform {
  constructor(private cssHelper: CssHelper) {}

  private cache: Record<string, any> = {};

  transform(value: any): any {
    if (!Object.keys(HealthColor).includes(value as HealthColor)) {
      return null;
    }
    if (!this.cache[value]) {
      this.cache[value] = { color: this.cssHelper.propertyValue(HealthColor[value]) };
    }
    return this.cache[value];
  }
}
