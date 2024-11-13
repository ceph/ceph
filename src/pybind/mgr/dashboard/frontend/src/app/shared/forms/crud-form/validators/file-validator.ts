import { AbstractControl } from '@angular/forms';

export function formlyAsyncFileValidator(control: AbstractControl): Promise<any> {
  return new Promise((resolve, _reject) => {
    if (control.value instanceof FileList) {
      control.value;
      let file = control.value[0];
      if (file.size > 4096) {
        resolve({ file_size: true });
      }
      resolve(null);
    }
    resolve({ not_a_file: true });
  });
}
