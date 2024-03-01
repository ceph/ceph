import { Component } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { FormGroup } from '@angular/forms';
import { FormlyFieldConfig, FormlyModule } from '@ngx-formly/core';

import { FormlyArrayTypeComponent } from './formly-array-type.component';
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
describe('FormlyArrayTypeComponent', () => {
  let component: MockFormComponent;
  let fixture: ComponentFixture<MockFormComponent>;

  configureTestBed({
    declarations: [FormlyArrayTypeComponent],
    imports: [
      FormlyModule.forRoot({
        types: [{ name: 'array', component: FormlyArrayTypeComponent }]
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
