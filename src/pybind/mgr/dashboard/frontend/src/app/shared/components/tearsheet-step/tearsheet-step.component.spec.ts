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
});
