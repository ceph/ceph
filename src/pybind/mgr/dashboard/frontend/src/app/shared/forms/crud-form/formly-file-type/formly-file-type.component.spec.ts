import { ComponentFixture, TestBed } from '@angular/core/testing';
import { FormControl } from '@angular/forms';
import { FormlyModule } from '@ngx-formly/core';

import { FormlyFileTypeComponent } from './formly-file-type.component';

describe('FormlyFileTypeComponent', () => {
  let component: FormlyFileTypeComponent;
  let fixture: ComponentFixture<FormlyFileTypeComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [FormlyModule.forRoot()],
      declarations: [FormlyFileTypeComponent]
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(FormlyFileTypeComponent);
    component = fixture.componentInstance;

    const formControl = new FormControl();
    const field = {
      key: 'file',
      type: 'file',
      templateOptions: {},
      get formControl() {
        return formControl;
      }
    };

    component.field = field;

    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
