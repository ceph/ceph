import { Injectable } from '@angular/core';
import _ from 'lodash';

@Injectable({
  providedIn: 'root'
})
export class StorageClassService {
  private _storageClassForm: Record<string, any> | null | undefined;

  get storageClassForm(): Record<string, any> | null | undefined {
    if (this._storageClassForm == null) {
      return this._storageClassForm;
    }
    return _.cloneDeep(this._storageClassForm);
  }

  set storageClassForm(form: Record<string, any> | null | undefined) {
    this._storageClassForm = form == null ? form : _.cloneDeep(form);
  }

  clearStorageClassForm(): void {
    this._storageClassForm = null;
  }

  hasStorageClassForm(): boolean {
    return this._storageClassForm != null;
  }
}
