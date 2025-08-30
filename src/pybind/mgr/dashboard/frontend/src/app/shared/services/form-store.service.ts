import { Injectable } from '@angular/core';
import { CdFormGroup } from '../forms/cd-form-group';
import { FormArray, FormControl } from '@angular/forms';

type FormData = Record<string, any>;

@Injectable({
  providedIn: 'root'
})
export class FormStoreService {
  private formData: FormData = null;

  save(form: CdFormGroup) {
    this.formData = form?.value;
  }

  load(): FormData {
    return this.formData;
  }

  clear() {
    this.formData = null;
  }

  // patchValue overset to handle complex types
  patchForm(form: CdFormGroup, data: Record<string, unknown>) {
    form.patchValue(data);
    Object.keys(data).forEach((itemName: string) => {
      // Handle FormArrays
      if (Array.isArray(data[itemName])) {
        const formArray = form.get(itemName) as FormArray;

        (data[itemName] as unknown[]).forEach((item: unknown) => {
          if (item) {
            switch (typeof item) {
              case 'string':
                formArray.push(new FormControl(item));
                break;
              /*
              TODO: add support for FormArray<object> for complex arrays like SMB cluster public_addrs
              // failing with Error: Control 'public_addrs,0,address' could not be found!
              case 'object':
                const itemObj = item as Record<string, unknown>;
                let group: Record<string, FormControl> = {};

                Object.keys(itemObj).forEach((key: string) => {
                  group.key = new FormControl(itemObj[key]);
                });
                formArray.push(new CdFormGroup(group));
                break;
               */
              default:
                break;
            }
          }
        });
      }
    });
  }
}
