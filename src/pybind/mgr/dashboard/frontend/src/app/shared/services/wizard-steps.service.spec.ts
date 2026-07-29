import { configureTestBed } from '~/testing/unit-test-helper';
import { TestBed } from '@angular/core/testing';

import { WizardStepsService } from './wizard-steps.service';

describe('WizardStepsService', () => {
  let service: WizardStepsService;

  configureTestBed({});

  beforeEach(() => {
    service = TestBed.inject(WizardStepsService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
