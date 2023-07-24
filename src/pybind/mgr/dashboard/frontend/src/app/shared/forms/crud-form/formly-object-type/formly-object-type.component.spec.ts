import { ComponentFixture, TestBed } from '@angular/core/testing';

import { FormlyObjectTypeComponent } from './formly-object-type.component';
import { FormlyFieldConfig, FormlyModule } from '@ngx-formly/core';
import { Component } from '@angular/core';
import { FormGroup } from '@angular/forms';

@Component({
  template: ` <form [formGroup]="form">
    <formly-form [model]="{}" [fields]="fields" [options]="{}" [form]="form"></formly-form>
  </form>`
})
class MockFormComponent {
  form = new FormGroup({});
  fields: FormlyFieldConfig[] = [
    {
      wrappers: ['object'],
      defaultValue: {}
    }
  ];
}

describe('FormlyObjectTypeComponent', () => {
  let fixture: ComponentFixture<MockFormComponent>;
  let mockComponent: MockFormComponent;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [FormlyObjectTypeComponent],
      imports: [
        FormlyModule.forRoot({
          types: [{ name: 'object', component: FormlyObjectTypeComponent }]
        })
      ]
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(MockFormComponent);
    mockComponent = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(mockComponent).toBeTruthy();
  });
});
