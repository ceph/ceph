import { Pipe, PipeTransform } from '@angular/core';

import { Color } from '../enum/color.enum';

@Pipe({
  name: 'healthColor'
})
export class HealthColorPipe implements PipeTransform {
  transform(value: any): any {
    return Color[value] ? { color: Color[value] } : null;
  }
}
