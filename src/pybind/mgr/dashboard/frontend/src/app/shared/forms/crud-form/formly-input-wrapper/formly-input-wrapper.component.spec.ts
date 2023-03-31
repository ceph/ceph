import { Component } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { FormGroup } from '@angular/forms';
import { FormlyFieldConfig, FormlyModule } from '@ngx-formly/core';

import { FormlyInputWrapperComponent } from './formly-input-wrapper.component';

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

describe('FormlyInputWrapperComponent', () => {
  let component: MockFormComponent;
  let fixture: ComponentFixture<MockFormComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [FormlyInputWrapperComponent],
      imports: [
        FormlyModule.forRoot({
          types: [{ name: 'input', component: FormlyInputWrapperComponent }]
        })
      ]
    }).compileComponents();
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
