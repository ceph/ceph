import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { configureTestBed } from '~/testing/unit-test-helper';
import { FormButtonPanelComponent } from './form-button-panel.component';

describe('FormButtonPanelComponent', () => {
  let component: FormButtonPanelComponent;
  let fixture: ComponentFixture<FormButtonPanelComponent>;

  configureTestBed({
    declarations: [FormButtonPanelComponent],
    schemas: [NO_ERRORS_SCHEMA]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(FormButtonPanelComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
