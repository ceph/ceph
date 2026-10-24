import { Component, Input } from '@angular/core';
import { NgForm } from '@angular/forms';

import { CdFormGroup } from '~/app/shared/forms/cd-form-group';

@Component({
  selector: 'cd-rgw-account-role-policy-fields',
  templateUrl: './rgw-account-role-policy-fields.component.html',
  standalone: false
})
export class RgwAccountRolePolicyFieldsComponent {
  @Input() formGroup: CdFormGroup;
  @Input() formDir: NgForm;
  @Input() fieldId = '';
  @Input() nameReadonly = false;
  @Input() showUniqueNameError = false;
  @Input() autofocus = false;
  @Input() rows = 12;
  @Input() namePlaceholder = '';
  @Input() docPlaceholder = '';
  @Input() nameHelperText = '';
  @Input() docHelperText = '';

  get nameInputId(): string {
    return this.fieldId ? `policy_name_${this.fieldId}` : 'policy_name';
  }

  get docInputId(): string {
    return this.fieldId ? `policy_doc_${this.fieldId}` : 'policy_doc';
  }

  showError(controlName: string, errorName: string): boolean {
    if (!this.formGroup || !this.formDir) {
      return false;
    }
    return this.formGroup.showError(controlName, this.formDir, errorName);
  }
}
