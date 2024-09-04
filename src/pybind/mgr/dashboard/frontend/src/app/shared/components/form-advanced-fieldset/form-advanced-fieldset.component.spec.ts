import { ComponentFixture, TestBed } from '@angular/core/testing';

import { FormAdvancedFieldsetComponent } from './form-advanced-fieldset.component';

describe('FormAdvancedFieldsetComponent', () => {
  let component: FormAdvancedFieldsetComponent;
  let fixture: ComponentFixture<FormAdvancedFieldsetComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [FormAdvancedFieldsetComponent]
    }).compileComponents();

    fixture = TestBed.createComponent(FormAdvancedFieldsetComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
