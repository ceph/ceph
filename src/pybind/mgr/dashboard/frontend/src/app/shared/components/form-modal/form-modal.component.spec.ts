import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule, Validators } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';

import { CdValidators } from '~/app/shared/forms/cd-validators';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed, FixtureHelper, FormHelper } from '~/testing/unit-test-helper';
import { FormModalComponent } from './form-modal.component';
import {
  CheckboxModule,
  ComboBoxModule,
  InputModule,
  ModalModule,
  NumberModule,
  SelectModule
} from 'carbon-components-angular';

describe('InputModalComponent', () => {
  let component: FormModalComponent;
  let fixture: ComponentFixture<FormModalComponent>;
  let fh: FixtureHelper;
  let formHelper: FormHelper;
  let submitted: object;

  const initialState = {
    titleText: 'Some title',
    message: 'Some description',
    fields: [
      {
        type: 'text',
        name: 'requiredField',
        value: 'some-value',
        required: true
      },
      {
        type: 'number',
        name: 'optionalField',
        label: 'Optional',
        errors: { min: 'Value has to be above zero!' },
        validators: [Validators.min(0), Validators.max(10)]
      },
      {
        type: 'binary',
        name: 'dimlessBinary',
        label: 'Size',
        value: 2048,
        validators: [CdValidators.binaryMin(1024), CdValidators.binaryMax(3072)]
      }
    ],
    submitButtonText: 'Submit button name',
    onSubmit: (values: object) => (submitted = values)
  };

  configureTestBed({
    imports: [
      RouterTestingModule,
      ReactiveFormsModule,
      SharedModule,
      InputModule,
      CheckboxModule,
      SelectModule,
      ComboBoxModule,
      NumberModule,
      ModalModule
    ]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(FormModalComponent);
    component = fixture.componentInstance;
    Object.assign(component, initialState);
    fixture.detectChanges();
    fh = new FixtureHelper(fixture);
    formHelper = new FormHelper(component.formGroup);
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('has the defined title', () => {
    fh.expectTextToBe('.cds--modal-header__heading', 'Some title');
  });

  it('has the defined description', () => {
    fh.expectTextToBe('[id=description]', 'Some description');
  });

  it('should display both inputs', () => {
    fh.expectElementVisible('#requiredField', true);
    fh.expectElementVisible('#optionalField', true);
  });

  it('has one defined label field', () => {
    fh.expectTextToBe('cds-number .cds--label', 'Optional');
  });

  it('has a predefined values for requiredField', () => {
    fh.expectFormFieldToBe('#requiredField', 'some-value');
  });

  it('gives back all form values on submit', () => {
    component.onSubmitForm(component.formGroup.value);
    expect(submitted).toEqual({
      dimlessBinary: 2048,
      requiredField: 'some-value',
      optionalField: null
    });
  });

  it('tests required field validation', () => {
    formHelper.expectErrorChange('requiredField', '', 'required');
  });

  it('tests required field message', () => {
    formHelper.setValue('requiredField', '', true);
    fh.expectTextToBe('.cds--form-requirement', 'This field is required.');
  });

  it('tests custom validator on number field', () => {
    formHelper.expectErrorChange('optionalField', -1, 'min');
    formHelper.expectErrorChange('optionalField', 11, 'max');
  });

  it('tests custom validator error message', () => {
    formHelper.setValue('optionalField', -1, true);
    fh.expectTextToBe('.cds--form-requirement', 'Value has to be above zero!');
  });

  it('tests default error message', () => {
    formHelper.setValue('optionalField', 11, true);
    fh.expectTextToBe('.cds--form-requirement', 'An error occurred.');
  });

  it('tests binary error messages', () => {
    formHelper.setValue('dimlessBinary', '4 K', true);
    fh.expectTextToBe('.cds--form-requirement', 'Size has to be at most 3 KiB or less');
    formHelper.setValue('dimlessBinary', '0.5 K', true);
    fh.expectTextToBe('.cds--form-requirement', 'Size has to be at least 1 KiB or more');
  });

  it('shows result of dimlessBinary pipe', () => {
    fh.expectFormFieldToBe('#dimlessBinary', '2 KiB');
  });

  it('changes dimlessBinary value and the result will still be a number', () => {
    formHelper.setValue('dimlessBinary', '3 K', true);
    component.onSubmitForm(component.formGroup.value);
    expect(submitted).toEqual({
      dimlessBinary: 3072,
      requiredField: 'some-value',
      optionalField: null
    });
  });
});
