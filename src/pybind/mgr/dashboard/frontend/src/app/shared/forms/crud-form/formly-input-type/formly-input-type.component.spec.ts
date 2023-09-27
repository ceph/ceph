import { Component } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { FormGroup } from '@angular/forms';
import { FormlyFieldConfig, FormlyModule } from '@ngx-formly/core';

import { FormlyInputTypeComponent } from './formly-input-type.component';
import { configureTestBed } from '~/testing/unit-test-helper';

@Component({
  template: ` <form [formGroup]="form">
    <formly-form [model]="{}" [fields]="fields" [options]="{}" [form]="form"></formly-form>
  </form>`
})
class MockFormComponent {
  form = new FormGroup({});
  fields: FormlyFieldConfig[] = [
    {
      wrappers: ['input'],
      defaultValue: {}
    }
  ];
}

describe('FormlyInputTypeComponent', () => {
  let component: MockFormComponent;
  let fixture: ComponentFixture<MockFormComponent>;

  configureTestBed({
    declarations: [FormlyInputTypeComponent],
    imports: [
      FormlyModule.forRoot({
        types: [{ name: 'input', component: FormlyInputTypeComponent }]
      })
    ]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(MockFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
