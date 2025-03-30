import { ComponentCanDeactivate } from '../services/unsaved-changes-guard.service';
import { CdForm } from './cd-form';
import { Observable } from 'rxjs';
import { CdFormGroup } from './cd-form-group';

export abstract class CdFormCanDeactivate extends CdForm implements ComponentCanDeactivate {
  form: CdFormGroup;  // Define the form property here

  canDeactivate(): boolean | Observable<boolean> {
    console.log('canDeactivate called');
    if (this.form?.dirty) {
      return confirm('There are unsaved changes. Do you want to leave?');
    }
    return true;
  }
}