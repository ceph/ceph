import { ComponentCanDeactivate } from '../services/unsaved-changes-guard.service';
import { CdForm } from './cd-form';
import { Observable } from 'rxjs';
import { CdFormGroup } from './cd-form-group';

export abstract class CdFormCanDeactivate extends CdForm implements ComponentCanDeactivate {
    abstract getFormGroup(): CdFormGroup;
    canDeactivate(): boolean | Observable<boolean> {
        const form = this.getFormGroup();
        if (form?.dirty) {
            return confirm('There are unsaved changes. Do you want to leave?');
        }
        return true;
    }
}