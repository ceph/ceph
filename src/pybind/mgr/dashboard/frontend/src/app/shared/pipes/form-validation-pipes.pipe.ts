import { Pipe, PipeTransform } from '@angular/core';
import { AbstractControl } from '@angular/forms';

/**
 * Usage: For setting the `invalid` property in carbon components
 *
 * e.g:
 * <cds-password-label
    [invalid]="userForm.get('newpassword') | hasError"
*/
@Pipe({ name: 'hasError', pure: true })
export class HasErrorPipe implements PipeTransform {
  transform(control: AbstractControl | null): boolean {
    return control ? control.invalid && (control.dirty || control.touched) : false;
  }
}

/**
 * Usage: For getting the name of error, which then can be used to show
 *        the desired error message
 * e.g:
 * <cds-password-label
   [invalidText]="this.INVALID_TEXTS[userForm.get('old-pasword') | getErrorName(['required', 'notmatch'])]">
  </cds-password-label>
 * INVALID_TEXTS: Map<Error Name, Error Message>
 */
@Pipe({ name: 'getError', pure: true })
export class GetErrorPipe implements PipeTransform {
  transform(control: AbstractControl | null, validationList: string[] = []): string {
    if (!control) return '';
    return validationList.find(validation => control.hasError(validation)) || '';
  }
}
