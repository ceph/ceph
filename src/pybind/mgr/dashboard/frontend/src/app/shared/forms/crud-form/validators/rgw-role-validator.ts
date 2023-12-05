import { AbstractControl } from '@angular/forms';

export function formlyRgwRolePath(control: AbstractControl): Promise<any> {
  return new Promise((resolve, _reject) => {
    if (control.value.match('^((\u002F)|(\u002F[\u0021-\u007E]+\u002F))$')) {
      resolve(null);
    }
    resolve({ rgwRolePath: true });
  });
}

export function formlyRgwRoleNameValidator(control: AbstractControl): Promise<any> {
  return new Promise((resolve, _reject) => {
    if (control.value.match('^[0-9a-zA-Z_+=,.@-]+$')) {
      resolve(null);
    }
    resolve({ rgwRoleName: true });
  });
}

export function formlyFormNumberValidator(control: AbstractControl): Promise<any> {
  return new Promise((resolve, _reject) => {
    if (control.value.match('^[0-9.]+$')) {
      if (control.value <= 12 && control.value >= 1) resolve(null);
    }
    resolve({ rgwRoleSessionDuration: true });
  });
}
