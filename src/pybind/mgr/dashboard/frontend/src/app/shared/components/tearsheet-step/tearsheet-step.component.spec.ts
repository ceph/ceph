import { ComponentFixture, TestBed } from '@angular/core/testing';

import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { TearsheetStepComponent } from './tearsheet-step.component';

describe('WizardComponent', () => {
  let component: TearsheetStepComponent;
  let fixture: ComponentFixture<TearsheetStepComponent>;

  configureTestBed({
    imports: [SharedModule]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(TearsheetStepComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should keep canProceed true by default when an inline form is projected', () => {
    component.formGroupDirective = {
      form: { valid: false }
    } as any;

    expect(component.autoDetectFormGroup).toBe(false);
    expect(component.canProceed).toBe(true);
  });

  it('should use inline form validity when autoDetectFormGroup is enabled', () => {
    component.autoDetectFormGroup = true;
    component.formGroupDirective = {
      form: { valid: false }
    } as any;

    expect(component.canProceed).toBe(false);

    component.formGroupDirective = {
      form: { valid: true }
    } as any;
    expect(component.canProceed).toBe(true);
  });
});
