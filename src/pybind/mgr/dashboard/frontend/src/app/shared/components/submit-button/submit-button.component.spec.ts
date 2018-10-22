import { ComponentFixture, TestBed } from '@angular/core/testing';
import { FormGroup } from '@angular/forms';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { SubmitButtonComponent } from './submit-button.component';

describe('SubmitButtonComponent', () => {
  let component: SubmitButtonComponent;
  let fixture: ComponentFixture<SubmitButtonComponent>;

  configureTestBed({
    declarations: [SubmitButtonComponent]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(SubmitButtonComponent);
    component = fixture.componentInstance;

    component.form = new FormGroup({}, {});

    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
