import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';

import { NgBootstrapFormValidationModule } from 'ng-bootstrap-form-validation';
import { BsModalRef, ModalModule } from 'ngx-bootstrap/modal';

import {
  configureTestBed,
  FixtureHelper,
  i18nProviders
} from '../../../../testing/unit-test-helper';
import { SharedModule } from '../../shared.module';
import { FormModalComponent } from './form-modal.component';

describe('InputModalComponent', () => {
  let component: FormModalComponent;
  let fixture: ComponentFixture<FormModalComponent>;
  let fh: FixtureHelper;
  let submitted;

  const initialState = {
    titleText: 'Some title',
    message: 'Some description',
    fields: [
      {
        type: 'inputText',
        name: 'requiredField',
        value: 'some-value',
        required: true
      },
      {
        type: 'inputText',
        name: 'optionalField',
        label: 'Optional'
      }
    ],
    submitButtonText: 'Submit button name',
    onSubmit: (values) => (submitted = values)
  };

  configureTestBed({
    imports: [
      ModalModule.forRoot(),
      NgBootstrapFormValidationModule.forRoot(),
      RouterTestingModule,
      ReactiveFormsModule,
      SharedModule
    ],
    providers: [i18nProviders, BsModalRef]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(FormModalComponent);
    component = fixture.componentInstance;
    Object.assign(component, initialState);
    fixture.detectChanges();
    fh = new FixtureHelper(fixture);
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('has the defined title', () => {
    fh.expectTextToBe('.modal-title', 'Some title');
  });

  it('has the defined description', () => {
    fh.expectTextToBe('.modal-body > p', 'Some description');
  });

  it('should display both inputs', () => {
    fh.expectElementVisible('#requiredField', true);
    fh.expectElementVisible('#optionalField', true);
  });

  it('has one defined label field', () => {
    fh.expectTextToBe('.col-form-label', 'Optional');
  });

  it('has a predefined values for requiredField', () => {
    fh.expectFormFieldToBe('#requiredField', 'some-value');
  });

  it('gives back all form values on submit', () => {
    component.onSubmitForm(component.formGroup.value);
    expect(submitted).toEqual({
      requiredField: 'some-value',
      optionalField: null
    });
  });
});
