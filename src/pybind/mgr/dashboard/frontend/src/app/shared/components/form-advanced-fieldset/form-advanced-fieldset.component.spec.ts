import { ComponentFixture, TestBed } from '@angular/core/testing';

import { FormAdvancedFieldsetComponent } from './form-advanced-fieldset.component';
import { CUSTOM_ELEMENTS_SCHEMA, NO_ERRORS_SCHEMA } from '@angular/core';

describe('FormAdvancedFieldsetComponent', () => {
  let component: FormAdvancedFieldsetComponent;
  let fixture: ComponentFixture<FormAdvancedFieldsetComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [FormAdvancedFieldsetComponent],
      schemas: [NO_ERRORS_SCHEMA, CUSTOM_ELEMENTS_SCHEMA]
    }).compileComponents();

    fixture = TestBed.createComponent(FormAdvancedFieldsetComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
