import { Pipe, PipeTransform } from '@angular/core';

/**
 * Convert the given value to a boolean value.
 */
@Pipe({
  name: 'boolean'
})
export class BooleanPipe implements PipeTransform {
  transform(value: any): boolean {
    let result = false;
    switch (value) {
      case true:
      case 1:
      case 'y':
      case 'yes':
      case 't':
      case 'true':
      case 'on':
      case '1':
        result = true;
        break;
    }
    return result;
  }
}
