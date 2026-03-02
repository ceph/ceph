import { Pipe, PipeTransform } from '@angular/core';

@Pipe({
  name: 'booleanText',
  standalone: false
})
export class BooleanTextPipe implements PipeTransform {
  transform(
    value: any,
    truthyText: string = $localize`Yes`,
    falsyText: string = $localize`No`
  ): string {
    return Boolean(value) ? truthyText : falsyText;
  }
}
