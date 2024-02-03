import { AbstractControl } from '@angular/forms';

export function formlyAsyncJsonValidator(control: AbstractControl): Promise<any> {
  return new Promise((resolve, _reject) => {
    try {
      JSON.parse(control.value);
      resolve(null);
    } catch (e) {
      resolve({ json: true });
    }
  });
}
