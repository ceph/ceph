import { Injectable } from '@angular/core';
import { CdFormGroup } from '../forms/cd-form-group';
import { FormArray, FormControl } from '@angular/forms';

type FormData = Record<string, any>;

@Injectable({
  providedIn: 'root'
})
export class FormStoreService {
  private formData: FormData = null;
  public clearFormData: boolean = true;

  save(form: CdFormGroup) {
    this.clearFormData = false;
    this.formData = form?.value;
  }

  load(): FormData {
    this.clearFormData = true;
    return this.formData;
  }

  clear() {
    if (this.clearFormData) {
      this.formData = null;
    }
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
            }
          }
        });
      }
    });
  }
}
