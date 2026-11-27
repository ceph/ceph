import { ComponentFixture, TestBed } from '@angular/core/testing';
import { FormControl, NgForm, ReactiveFormsModule, Validators } from '@angular/forms';

import { configureTestBed } from '~/testing/unit-test-helper';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { SharedModule } from '~/app/shared/shared.module';
import { RgwAccountRolePolicyFieldsComponent } from './rgw-account-role-policy-fields.component';
import { InputModule } from 'carbon-components-angular';

describe('RgwAccountRolePolicyFieldsComponent', () => {
  let component: RgwAccountRolePolicyFieldsComponent;
  let fixture: ComponentFixture<RgwAccountRolePolicyFieldsComponent>;
  let formGroup: CdFormGroup;

  configureTestBed({
    imports: [ReactiveFormsModule, SharedModule, InputModule],
    declarations: [RgwAccountRolePolicyFieldsComponent]
  });

  beforeEach(() => {
    formGroup = new CdFormGroup({
      policy_name: new FormControl('', Validators.required),
      policy_doc: new FormControl('', Validators.required)
    });

    fixture = TestBed.createComponent(RgwAccountRolePolicyFieldsComponent);
    component = fixture.componentInstance;
    component.formGroup = formGroup;
    component.formDir = { submitted: false } as NgForm;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should build input ids from fieldId', () => {
    component.fieldId = 'edit';
    expect(component.nameInputId).toBe('policy_name_edit');
    expect(component.docInputId).toBe('policy_doc_edit');
  });

  it('should show error only when form reports it', () => {
    spyOn(formGroup, 'showError').and.returnValue(true);
    expect(component.showError('policy_name', 'required')).toBe(true);
    expect(formGroup.showError).toHaveBeenCalledWith('policy_name', component.formDir, 'required');
  });

  it('should not show error when formGroup or formDir is missing', () => {
    component.formGroup = null;
    expect(component.showError('policy_name', 'required')).toBe(false);
  });
});
